"""Offline retention/recovery protocol, NOT a GitHub adapter or publisher.

Backend metadata, quota usage, scope and receipts must be authenticated by a
future adapter. Self-reported manifests cannot provide that evidence. No archive
extraction, StateStore adoption, network access or distributed CAS lives here.
"""
import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from typing import Protocol

from crypto_boom.okx_state import Checkpoint
from crypto_boom.okx_trial import HOUR, MINUTE, VERSION

MIB = 1024 * 1024
GIB = 1024 * MIB
DAY = 24 * HOUR
BUNDLE_LIMIT = 9 * MIB
WIRE = "okx-storage-bundle-v1"


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _integer(value, minimum=0):
    return type(value) is int and value >= minimum


def _hex(value, length=64):
    return isinstance(value, str) and re.fullmatch(f"[0-9a-f]{{{length}}}", value) is not None


@dataclass(frozen=True)
class Scope:
    repository: str
    workflow_id: int
    ref: str

    def __post_init__(self):
        if (not isinstance(self.repository, str) or len(self.repository) > 256
                or re.fullmatch(r"[\w.-]+/[\w.-]+", self.repository, re.ASCII) is None
                or not _integer(self.workflow_id, 1) or not isinstance(self.ref, str)
                or not self.ref.startswith("refs/heads/") or not 12 <= len(self.ref) <= 256):
            raise ValueError("invalid_scope")


@dataclass(frozen=True)
class Snapshot:
    """Admitted carrier metadata; confirmed/validated are adapter evidence, not wire claims.

    size is actual provider quota size (including archive overhead), not JSON size.
    The fake tests deliberately reserve worst-case sizes without allocating them.
    Market validated means ProductCache content validation was performed upstream.
    """
    object_id: str
    kind: str
    scope: Scope
    source_sha: str
    run_id: int
    attempt: int
    generation: int
    cutoff: int
    created_at: int
    expires_at: int
    sha256: str
    size: int
    parent_digest: str | None = None
    confirmed: bool = False
    validated: bool = False

    def __post_init__(self):
        if (not isinstance(self.object_id, str)
                or re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", self.object_id) is None
                or self.kind not in ("state", "market") or not isinstance(self.scope, Scope)
                or not _hex(self.source_sha, 40) or not _integer(self.run_id, 1)
                or not _integer(self.attempt, 1) or not _integer(self.generation)
                or not _integer(self.cutoff) or self.cutoff % MINUTE
                or not _integer(self.created_at) or self.created_at < self.cutoff
                or not _integer(self.expires_at) or self.expires_at <= self.created_at
                or not _hex(self.sha256) or not _integer(self.size, 1)
                or (self.parent_digest is not None and not _hex(self.parent_digest))
                or type(self.confirmed) is not bool or type(self.validated) is not bool
                or (self.kind == "state" and self.expires_at > self.created_at + DAY)):
            raise ValueError("invalid_snapshot")


@dataclass(frozen=True)
class Inventory:
    objects: tuple[Snapshot, ...]
    complete: bool
    cache_usage: int | None  # Whole repository, not only this workflow.
    billable_usage: int | None  # Whole account artifacts + Packages.
    cache_free_write: bool | None = None
    artifact_free_write: bool | None = None
    # Adapter evidence: free configuration, stop-usage budgets, accrued billing
    # allowance and permission. Instantaneous usage alone does NOT establish it.

    def __post_init__(self):
        if (not isinstance(self.objects, tuple) or len(self.objects) > 1000
                or any(not isinstance(item, Snapshot) for item in self.objects)
                or len({item.object_id for item in self.objects}) != len(self.objects)
                or type(self.complete) is not bool
                or any(value is not None and not _integer(value)
                       for value in (self.cache_usage, self.billable_usage))
                or any(value is not None and type(value) is not bool
                       for value in (self.cache_free_write, self.artifact_free_write))):
            raise ValueError("invalid_inventory")
        # Inventory can omit account/repository foreign objects, never their usage.
        for kind, usage in (("market", self.cache_usage), ("state", self.billable_usage)):
            if usage is not None and usage < sum(x.size for x in self.objects if x.kind == kind):
                raise ValueError("underreported_usage")


@dataclass(frozen=True)
class Receipt:
    scope: Scope
    generation: int
    published_at: int
    ids: tuple[str, ...]
    batch: str

    def __post_init__(self):
        if (not isinstance(self.scope, Scope) or not _integer(self.generation)
                or not _integer(self.published_at) or not isinstance(self.ids, tuple)
                or not 0 < len(self.ids) <= 10000 or len(set(self.ids)) != len(self.ids)
                or any(not _hex(key) for key in self.ids) or not _hex(self.batch)
                or self.batch != digest(_json(tuple(sorted(self.ids))))):
            raise ValueError("invalid_receipt")


@dataclass(frozen=True)
class ReceiptQuery:
    """Complete authenticated default-branch query; timestamps are milliseconds."""
    scope: Scope
    start: int
    end: int
    complete: bool
    receipts: tuple[Receipt, ...] = ()

    def __post_init__(self):
        if (not isinstance(self.scope, Scope) or not _integer(self.start)
                or not _integer(self.end) or self.end < self.start
                or type(self.complete) is not bool or not isinstance(self.receipts, tuple)
                or len(self.receipts) > 1000 or any(not isinstance(r, Receipt) for r in self.receipts)
                or sum(len(r.ids) for r in self.receipts) > 10000
                or any(r.scope != self.scope or not self.start <= r.published_at <= self.end
                       for r in self.receipts)):
            raise ValueError("invalid_receipt_query")

    def covers(self, scope: Scope, now: int) -> bool:
        return self.complete and self.scope == scope and self.start <= max(0, now - 2 * DAY) and self.end == now


@dataclass(frozen=True)
class Recovery:
    checkpoint: Checkpoint | None
    reason: str
    may_prepare: bool = False  # Diagnostic only, never a publication capability.
    snapshot: Snapshot | None = None


@dataclass(frozen=True)
class SaveResult:
    reason: str
    confirmed: Snapshot | None = None
    cleanup_failed: tuple[str, ...] = ()


class Backend(Protocol):
    """Effects owned by a future authenticated adapter; fake implementation in tests.

    read must bound transport/decoded bytes before allocation. upload returns None
    for unknown outcome, never retries/overwrites. delete True is a confirmed
    deletion; coordinator still refreshes usage rather than granting quota credit.
    Unknown writes must remain discoverable as unconfirmed inventory entries, or
    make inventory incomplete, until the adapter has reconciled their outcome.
    """
    def inventory(self) -> Inventory: ...
    def read(self, object_id: str, limit: int) -> bytes: ...
    def upload(self, snapshot: Snapshot, body: bytes) -> Snapshot | None: ...
    def delete(self, object_id: str) -> bool: ...


def _strict_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate_json_key")
        value[key] = item
    return value


def _unpack(raw: bytes):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= BUNDLE_LIMIT:
        raise ValueError("bundle_size")
    body = json.loads(raw, object_pairs_hook=_strict_object)
    expected = {"schema", "rules_version", "scope", "source_sha", "run_id", "attempt",
                "generation", "cutoff", "created_at", "parent_digest", "state_sha256", "state"}
    if not isinstance(body, dict) or set(body) != expected:
        raise ValueError("bundle_fields")
    if body["schema"] != WIRE or body["rules_version"] != VERSION:
        raise ValueError("bundle_version")
    scope = Scope(**body["scope"])
    # Snapshot owns transport metadata validation; Checkpoint owns event validation.
    record = Snapshot("decoded", "state", scope, body["source_sha"], body["run_id"], body["attempt"],
                      body["generation"], body["cutoff"], body["created_at"], body["created_at"] + DAY,
                      digest(raw), len(raw), body["parent_digest"])
    if body["state_sha256"] != digest(_json(body["state"])):
        raise ValueError("state_digest")
    checkpoint = Checkpoint.decode(body["state"])
    if (checkpoint.generation != record.generation or checkpoint.at != record.cutoff
            or set(body["state"]) != set(checkpoint.encode())):
        raise ValueError("state_header")
    return record, checkpoint


def _same_payload(left: Snapshot, right: Snapshot):
    # Provider expiry can shorten the logical lifetime, never extend it. Snapshot
    # admission enforces that cap; expiry is not part of the immutable bundle.
    return replace(left, object_id="decoded", size=1, expires_at=left.created_at + DAY,
                   confirmed=False, validated=False) == replace(
        right, object_id="decoded", size=1, expires_at=right.created_at + DAY,
        confirmed=False, validated=False)


def decode_state_bundle(raw: bytes, *, scope: Scope, source_sha: str, run_id: int,
                        attempt: int) -> tuple[Snapshot, Checkpoint]:
    """Bind wire metadata to independently authenticated provider evidence."""
    if (not isinstance(scope, Scope) or not _hex(source_sha, 40)
            or not _integer(run_id, 1) or not _integer(attempt, 1)):
        raise ValueError("invalid_bundle_authority")
    record, checkpoint = _unpack(raw)
    if (record.scope != scope or record.source_sha != source_sha
            or record.run_id != run_id or record.attempt != attempt):
        raise ValueError("bundle_producer_mismatch")
    return record, checkpoint


def _read_state(backend: Backend, record: Snapshot):
    if record.size > 10 * MIB:
        raise ValueError("state_object_capacity")
    raw = backend.read(record.object_id, BUNDLE_LIMIT)
    decoded, checkpoint = _unpack(raw)
    if not _same_payload(record, decoded):
        raise ValueError("carrier_bundle_mismatch")
    return checkpoint


def _inheritance(parent: Checkpoint, child: Checkpoint):
    if child.generation != parent.generation + 1 or child.at < parent.at:
        raise ValueError("nonmonotonic_state")
    for key, before in parent.ledger.items():
        after = child.ledger.get(key)
        if before["status"] == "confirmed" and child.at - before["prepared_at"] > 2 * DAY and after is None:
            continue
        if after is None or {k: v for k, v in before.items() if k != "status"} != {
                k: v for k, v in after.items() if k != "status"}:
            raise ValueError("lost_ledger_identity")
        if (before["status"] == "confirmed" and after["status"] != "confirmed"
                or before["status"] == "unknown" and after["status"] == "pending"):
            raise ValueError("ledger_regression")


def _reconcile(checkpoint: Checkpoint, query: ReceiptQuery):
    for receipt in query.receipts:
        if receipt.generation > checkpoint.generation:
            raise ValueError("receipt_ahead_of_state")
        if any(key not in checkpoint.ledger for key in receipt.ids):
            raise ValueError("receipt_missing_intent")
        if any(checkpoint.ledger[key]["prepared_at"] > receipt.published_at for key in receipt.ids):
            raise ValueError("receipt_before_intent")
        checkpoint.reconcile(receipt.ids, receipt.batch)


def _prove_promotions(parent: Checkpoint, child: Checkpoint, query: ReceiptQuery):
    promoted = {key for key, before in parent.ledger.items()
                if before["status"] != "confirmed" and child.ledger[key]["status"] == "confirmed"}
    if promoted:
        proof = Checkpoint.decode(json.loads(_json(parent.encode())))
        # Receipt belongs to the newer child generation, not necessarily the parent.
        proof.generation = child.generation
        relevant = replace(query, receipts=tuple(r for r in query.receipts if promoted.intersection(r.ids)))
        _reconcile(proof, relevant)
        if any(proof.ledger[key]["status"] != "confirmed" for key in promoted):
            raise ValueError("promotion_without_receipt")


def pack_state(checkpoint: Checkpoint, *, scope: Scope, source_sha: str, run_id: int,
               attempt: int, created_at: int, parent: bytes | None = None,
               receipts: ReceiptQuery | None = None) -> bytes:
    """Build an immutable candidate; caller still must obtain upload confirmation."""
    checked = Checkpoint.decode(json.loads(_json(checkpoint.encode())))
    parent_digest = None
    if parent is None:
        if checked.generation != 0 or checked.notifications_enabled or checked.ledger:
            raise ValueError("explicit_disabled_baseline_required")
    else:
        previous, state = _unpack(parent)
        if previous.scope != scope or created_at < previous.created_at:
            raise ValueError("parent_scope_or_time")
        _inheritance(state, checked)
        if any(v["status"] != "confirmed" and checked.ledger[k]["status"] == "confirmed"
               for k, v in state.ledger.items()):
            if receipts is None or not receipts.covers(scope, created_at):
                raise ValueError("promotion_requires_complete_receipts")
            _prove_promotions(state, checked, receipts)
        parent_digest = digest(parent)
    if receipts is not None and receipts.covers(scope, created_at):
        _reconcile(Checkpoint.decode(json.loads(_json(checked.encode()))), receipts)
    body = {"schema": WIRE, "rules_version": VERSION, "scope": asdict(scope), "source_sha": source_sha,
            "run_id": run_id, "attempt": attempt, "generation": checked.generation, "cutoff": checked.at,
            "created_at": created_at, "parent_digest": parent_digest,
            "state_sha256": digest(_json(checked.encode())), "state": checked.encode()}
    raw = _json(body)
    _unpack(raw)
    return raw


def state_candidate(object_id: str, raw: bytes, billable_size: int) -> Snapshot:
    decoded, _ = _unpack(raw)
    if not _integer(billable_size, 1):
        raise ValueError("invalid_billable_size")
    return replace(decoded, object_id=object_id, size=billable_size)


def recover_state(backend: Backend, *, scope: Scope, allowed_sources: frozenset[str], now: int,
                  receipts: ReceiptQuery | None = None, minimum_generation: int = 0) -> Recovery:
    """Never silently roll back a corrupt/missing frontier to an older notifying state."""
    if (not isinstance(scope, Scope) or not _integer(now) or not _integer(minimum_generation)
            or not isinstance(allowed_sources, frozenset)
            or not allowed_sources or len(allowed_sources) > 100 or any(not _hex(s, 40) for s in allowed_sources)):
        raise ValueError("invalid_restore_authority")
    try:
        inventory = backend.inventory()
        if not inventory.complete:
            return Recovery(None, "inventory_incomplete")
        records = [x for x in inventory.objects if x.scope == scope and x.kind == "state"]
        if not records:
            return Recovery(None, "state_missing")
        latest = max(x.generation for x in records)
        if latest < minimum_generation:
            return Recovery(None, "state_frontier_missing")
        frontier = [x for x in records if x.generation == latest]
        if len({x.sha256 for x in frontier}) != 1:
            return Recovery(None, "state_frontier_conflict")
        record = max(frontier, key=lambda x: x.created_at)
        if not record.confirmed or record.created_at > now or record.expires_at <= now:
            return Recovery(None, "state_frontier_unavailable")
        if record.source_sha not in allowed_sources:
            return Recovery(None, "state_source_incompatible")
        checkpoint = _read_state(backend, record)
        parent = None
        if record.generation:
            parents = [x for x in records if x.generation == record.generation - 1]
            if not parents or len({x.sha256 for x in parents}) != 1:
                return Recovery(None, "state_parent_missing_or_conflicting")
            previous = parents[0]
            if (not previous.confirmed or previous.sha256 != record.parent_digest
                    or previous.source_sha not in allowed_sources or previous.created_at > record.created_at):
                return Recovery(None, "state_parent_mismatch")
            parent = _read_state(backend, previous)
            _inheritance(parent, checkpoint)
        elif record.parent_digest is not None or checkpoint.notifications_enabled or checkpoint.ledger:
            return Recovery(None, "state_baseline_invalid")
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as exc:
        return Recovery(None, f"state_invalid:{type(exc).__name__}")
    # Work only on newly decoded copies. An incomplete receipt query closes the
    # diagnostic gate, even if the checkpoint had no unresolved publication.
    if receipts is None or not receipts.covers(scope, now):
        checkpoint.notifications_enabled = False
        return Recovery(checkpoint, "receipts_incomplete", snapshot=record)
    try:
        if parent is not None:
            _prove_promotions(parent, checkpoint, receipts)
        _reconcile(checkpoint, receipts)
    except (ValueError, KeyError, TypeError) as exc:
        checkpoint.notifications_enabled = False
        return Recovery(checkpoint, f"receipts_invalid:{type(exc).__name__}", snapshot=record)
    unresolved = any(x["status"] != "confirmed" for x in checkpoint.ledger.values())
    enabled = checkpoint.notifications_enabled and not unresolved
    checkpoint.notifications_enabled = enabled
    return Recovery(checkpoint, "publication_unresolved" if unresolved else "restored", enabled, record)


def market_candidate(inventory: Inventory, scope: Scope, now: int) -> Snapshot | None:
    """Cache miss is rebuildable. Archive contents still require ProductCache checks."""
    if not inventory.complete:
        return None
    records = [x for x in inventory.objects if x.scope == scope and x.kind == "market"
               and x.confirmed and x.validated and x.size <= 600 * MIB and x.created_at <= now < x.expires_at]
    return max(records, key=lambda x: (x.cutoff, x.created_at), default=None)


def quota_reason(inventory: Inventory, candidate: Snapshot) -> str | None:
    if not inventory.complete:
        return "inventory_incomplete"
    usage = inventory.cache_usage if candidate.kind == "market" else inventory.billable_usage
    if usage is None:
        return "usage_unknown"
    free_write = inventory.cache_free_write if candidate.kind == "market" else inventory.artifact_free_write
    if free_write is not True:
        return "free_write_unknown" if free_write is None else "free_write_denied"
    object_limit, project_limit, global_limit = ((600 * MIB, 2 * GIB, 8 * GIB)
                                                if candidate.kind == "market" else (10 * MIB, 64 * MIB, 384 * MIB))
    if candidate.size > object_limit:
        return "object_capacity"
    project_usage = sum(x.size for x in inventory.objects if x.scope == candidate.scope and x.kind == candidate.kind)
    if project_usage + candidate.size > project_limit:
        return "project_capacity"
    if usage + candidate.size > global_limit:
        return "shared_capacity"
    return None


def _prune(backend: Backend, inventory: Inventory, scope: Scope, kind: str) -> tuple[str, ...]:
    if not inventory.complete:
        return ()
    records = [x for x in inventory.objects if x.scope == scope and x.kind == kind]
    generations = sorted({x.generation for x in records}, reverse=True)
    protected = set(generations[:2])
    # Do not destroy recovery evidence when a frontier is conflicted/unconfirmed.
    if any(len({x.sha256 for x in records if x.generation == g}) != 1
           or any(not x.confirmed for x in records if x.generation == g) for g in protected):
        return ()
    try:
        protected_states = {}
        for record in records:
            if record.generation in protected:
                if kind == "state":
                    protected_states[record.generation] = _read_state(backend, record)
                elif not record.validated:
                    return ()
        if kind == "state" and len(protected) == 2:
            newest, previous = generations[:2]
            latest = next(x for x in records if x.generation == newest)
            parent = next(x for x in records if x.generation == previous)
            if latest.parent_digest != parent.sha256 or latest.created_at < parent.created_at:
                return ()
            _inheritance(protected_states[previous], protected_states[newest])
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        return ()
    failed = []
    for record in records:
        if record.generation in protected or not record.confirmed:
            continue
        try:
            if kind == "state":
                _read_state(backend, record)
            elif not record.validated:
                continue
            if not backend.delete(record.object_id):
                failed.append(record.object_id)
        except (OSError, ValueError, KeyError, TypeError, RecursionError):
            failed.append(record.object_id)
    return tuple(failed)


def save_snapshot(backend: Backend, candidate: Snapshot, body: bytes, *, now: int,
                  bootstrap: bool = False, receipts: ReceiptQuery | None = None) -> SaveResult:
    """Single-writer local protocol. A real adapter must fence concurrent writers.

    No retries of uploads, no deletion on an unknown upload result, and no quota
    credit until a fresh usage query observes it. Only confirmed uploads may prune.
    """
    if (not _integer(now) or not isinstance(body, bytes) or digest(body) != candidate.sha256
            or (candidate.kind == "market" and len(body) > candidate.size)
            or candidate.confirmed or candidate.created_at > now or candidate.expires_at <= now
            or type(bootstrap) is not bool):
        raise ValueError("invalid_upload_candidate")
    checked = None
    if candidate.kind == "state":
        decoded, checked = _unpack(body)
        if not _same_payload(candidate, decoded):
            raise ValueError("candidate_bundle_mismatch")
    elif not candidate.validated:
        return SaveResult("market_validation_required")
    failed = ()
    try:
        inventory = backend.inventory()
        if not inventory.complete:
            return SaveResult("inventory_incomplete")
        matches = [x for x in inventory.objects if x.object_id == candidate.object_id]
        if matches:
            existing = matches[0]
            if not _same_payload(existing, candidate) or existing.size != candidate.size:
                return SaveResult("identity_collision")
            if existing.expires_at <= now:
                return SaveResult("existing_expired")
            if not existing.confirmed:
                return SaveResult("upload_requires_reconciliation")
            if candidate.kind == "state":
                _read_state(backend, existing)
            elif not existing.validated:
                return SaveResult("market_validation_required")
            return SaveResult("already_confirmed", existing)
        records = [x for x in inventory.objects if x.scope == candidate.scope and x.kind == candidate.kind]
        if records:
            generation = max(x.generation for x in records)
            frontier = [x for x in records if x.generation == generation]
            if (candidate.generation != generation + 1 or len({x.sha256 for x in frontier}) != 1
                    or any(not x.confirmed for x in frontier)):
                return SaveResult("stale_or_conflicting_frontier")
            previous = frontier[0]
            if candidate.cutoff < previous.cutoff or candidate.created_at < previous.created_at:
                return SaveResult("nonmonotonic_snapshot")
            if candidate.kind == "market":
                interval = 10 * MINUTE if bootstrap else HOUR
                if candidate.created_at - previous.created_at < interval:
                    return SaveResult("market_cadence")
            else:
                if previous.expires_at <= now:
                    return SaveResult("state_frontier_expired")
                if candidate.parent_digest != previous.sha256:
                    return SaveResult("parent_mismatch")
                parent = _read_state(backend, previous)
                if previous.generation:
                    ancestors = [x for x in records if x.generation == previous.generation - 1]
                    if (not ancestors or len({x.sha256 for x in ancestors}) != 1
                            or any(not x.confirmed for x in ancestors)
                            or previous.parent_digest != ancestors[0].sha256):
                        return SaveResult("state_parent_missing_or_conflicting")
                    _inheritance(_read_state(backend, ancestors[0]), parent)
                _inheritance(parent, checked)
                if receipts is not None and receipts.covers(candidate.scope, now):
                    _prove_promotions(parent, checked, receipts)
                    _reconcile(Checkpoint.decode(json.loads(_json(checked.encode()))), receipts)
                elif any(v["status"] != "confirmed" and checked.ledger[k]["status"] == "confirmed"
                         for k, v in parent.ledger.items()):
                    return SaveResult("promotion_requires_complete_receipts")
        elif candidate.generation != 0 or candidate.parent_digest is not None:
            return SaveResult("state_or_cache_frontier_missing")
        failed = _prune(backend, inventory, candidate.scope, candidate.kind)
        # Deliberately requery; deleted bytes are NOT subtracted locally.
        inventory = backend.inventory()
        current = [x for x in inventory.objects if x.scope == candidate.scope and x.kind == candidate.kind]
        previous_generation = max((r.generation for r in records), default=-1)
        current_generation = max((r.generation for r in current), default=-1)
        before = {(x.generation, x.sha256, x.confirmed) for x in records if x.generation == previous_generation}
        after = {(x.generation, x.sha256, x.confirmed) for x in current if x.generation == current_generation}
        if before != after:
            return SaveResult("frontier_changed", cleanup_failed=failed)
        if len(current) >= 3:
            return SaveResult("retention_backlog", cleanup_failed=failed)
        refusal = quota_reason(inventory, candidate)
        if refusal:
            return SaveResult(refusal, cleanup_failed=failed)
        confirmed = backend.upload(candidate, body)
        if confirmed is None:
            return SaveResult("upload_unknown", cleanup_failed=failed)
        if (not confirmed.confirmed or confirmed.expires_at <= now or not _same_payload(confirmed, candidate)
                or confirmed.object_id != candidate.object_id or confirmed.size != candidate.size):
            return SaveResult("upload_confirmation_mismatch", cleanup_failed=failed)
        if candidate.kind == "state":
            _read_state(backend, confirmed)
        elif not confirmed.validated:
            return SaveResult("upload_confirmation_mismatch", cleanup_failed=failed)
    except (OSError, ValueError, KeyError, TypeError, RecursionError) as exc:
        return SaveResult(f"storage_failed:{type(exc).__name__}", cleanup_failed=failed)
    try:
        failed += _prune(backend, backend.inventory(), candidate.scope, candidate.kind)
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        return SaveResult("saved_cleanup_unavailable", confirmed, failed)
    return SaveResult("saved_degraded" if failed else "saved", confirmed, failed)
