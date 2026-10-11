"""Local event checkpoint and publication-intent ledger. No remote publisher.

A public receipt confirms a batch identity, not email receipt. Missing state is
fail-closed; rebuilding price cache is never authorization to send old events.
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .okx_trial import (
    HOUR,
    MINUTE,
    RULES,
    VERSION,
    Event,
    EventTracker,
    Transition,
    Unavailable,
)

SCHEMA = "okx-checkpoint-v1"


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _minute(value):
    return type(value) is int and value >= 0 and value % MINUTE == 0


@dataclass
class Checkpoint:
    generation: int = 0
    at: int = 0
    cursor: int = 0
    notifications_enabled: bool = False
    tracker: EventTracker = field(default_factory=EventTracker)
    ledger: dict[str, dict] = field(default_factory=dict)

    def prepare(self, transitions: list[Transition], at: int) -> tuple[str, ...]:
        if not _minute(at):
            raise ValueError("Invalid prepare time")
        if not self.notifications_enabled:
            raise Unavailable("state_baseline_or_reconciliation_required")
        if any(item["status"] in ("pending", "unknown") for item in self.ledger.values()):
            raise Unavailable("unresolved_publication")
        # Only confirmed records age out; unresolved intent is never discarded.
        self.ledger = {k: v for k, v in self.ledger.items()
                       if v["status"] != "confirmed" or at - v["prepared_at"] <= 48 * HOUR}
        messages = {}
        for transition in transitions:
            if transition.stage not in ("OBSERVED", "PERSISTENT") or not _minute(transition.at):
                continue
            if not 0 <= at - transition.at <= 24 * HOUR:
                continue
            identity = [transition.identity, transition.stage]
            key = _digest(identity)
            if key not in self.ledger and key not in messages:
                if len(self.ledger) + len(messages) >= 10000:
                    raise Unavailable("ledger_capacity")
                messages[key] = {"identity": identity, "prepared_at": at, "status": "pending",
                                 "batch": "", "transition_at": transition.at}
        if len(self.ledger) + len(messages) > 10000:
            raise Unavailable("ledger_capacity")
        ids = tuple(sorted(messages))
        batch = _digest(ids) if ids else ""
        for key, message in messages.items():
            message["batch"] = batch
            self.ledger[key] = message
        return ids

    def unknown(self, ids: tuple[str, ...]):
        for key in ids:
            if key not in self.ledger or self.ledger[key]["status"] != "pending":
                raise ValueError("Unknown result must refer to pending intent")
        for key in ids:
            self.ledger[key]["status"] = "unknown"

    def reconcile(self, ids: tuple[str, ...], receipt_batch: str):
        if not ids or len(set(ids)) != len(ids) or receipt_batch != _digest(tuple(sorted(ids))):
            raise ValueError("Receipt identity mismatch")
        if any(key not in self.ledger or self.ledger[key]["batch"] != receipt_batch for key in ids):
            raise ValueError("Receipt does not match saved intent")
        expected = {k for k, v in self.ledger.items() if v["batch"] == receipt_batch}
        if set(ids) != expected:
            raise ValueError("Partial receipt cannot confirm batch")
        for key in ids:
            self.ledger[key]["status"] = "confirmed"

    def encode(self):
        return {"schema": SCHEMA, "rules_version": VERSION, "generation": self.generation,
                "at": self.at, "cursor": self.cursor, "notifications_enabled": self.notifications_enabled,
                "last_at": [[rule, product, at] for (rule, product), at in sorted(self.tracker.last_at.items())],
                "active": [asdict(event) for _, event in sorted(self.tracker.active.items())], "ledger": self.ledger}

    @classmethod
    def decode(cls, body):
        if body["schema"] != SCHEMA or body["rules_version"] != VERSION:
            raise ValueError("Incompatible state")
        if (type(body["generation"]) is not int or body["generation"] < 0 or not _minute(body["at"])
                or type(body["cursor"]) is not int or body["cursor"] < 0
                or type(body["notifications_enabled"]) is not bool):
            raise ValueError("Invalid state header")
        result = cls(body["generation"], body["at"], body["cursor"], body["notifications_enabled"])
        if len(body["last_at"]) > 4000 or len(body["active"]) > 4000 or len(body["ledger"]) > 10000:
            raise ValueError("State entry limit")
        for rule, product, at in body["last_at"]:
            key = (rule, _product_key(product))
            if rule not in RULES or not _minute(at) or at > result.at or key in result.tracker.last_at:
                raise ValueError("Invalid checkpoint boundary")
            result.tracker.last_at[key] = at
        for raw in body["active"]:
            event = Event(**{**raw, "product": _product_key(raw["product"])})
            key = (event.rule, event.product)
            last = result.tracker.last_at.get(key)
            if (last is None or key in result.tracker.active or event.rule not in RULES
                    or event.direction not in ("UP", "DOWN") or event.stage not in ("OBSERVED", "PERSISTENT", "UNKNOWN")
                    or not _minute(event.first_at) or not _minute(event.last_qualified_at)
                    or not event.first_at <= event.last_qualified_at <= last
                    or type(event.false_count) is not int or not 0 <= event.false_count < 10
                    or type(event.persistent_announced) is not bool or event.end_reason is not None
                    or (event.stage == "PERSISTENT" and not event.persistent_announced)
                    or (event.continuous_anchor is not None and (not _minute(event.continuous_anchor)
                        or not event.first_at <= event.continuous_anchor <= event.last_qualified_at))):
                raise ValueError("Invalid active event")
            result.tracker.active[key] = event
        if not isinstance(body["ledger"], dict):
            raise ValueError("Invalid ledger")
        for key, item in body["ledger"].items():
            if (not isinstance(item, dict) or key != _digest(item["identity"])
                    or item["status"] not in ("pending", "unknown", "confirmed")
                    or not _minute(item["prepared_at"]) or not _minute(item["transition_at"])
                    or item["prepared_at"] > result.at or item["transition_at"] > item["prepared_at"]
                    or not isinstance(item["batch"], str) or len(item["batch"]) != 64):
                raise ValueError("Invalid ledger entry")
            result.ledger[key] = item
        return result


def _product_key(raw):
    if (not isinstance(raw, (list, tuple)) or len(raw) != 3 or raw[0] != "okx"
            or raw[1] not in ("SPOT", "SWAP") or not isinstance(raw[2], str) or not 0 < len(raw[2]) <= 128):
        raise ValueError("Invalid product key")
    return tuple(raw)


class StateStore:
    """Exclusive local writer and generation-fenced atomic checkpoint.

    A stale lock is diagnostic, never automatically stolen. This is not an
    Actions artifact loader, distributed lock, or exactly-once delivery claim.
    """
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.locked = False
        self.writer_token = None
        self.retain_lock = False

    def _owns_lock(self):
        try:
            with self.lock_path.open("rb") as stream:
                return self.writer_token is not None and stream.read(33) == self.writer_token.encode()
        except OSError:
            return False

    @contextmanager
    def writer(self):
        try:
            handle = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise Unavailable("writer_lock_present") from exc
        self.writer_token = uuid.uuid4().hex
        self.retain_lock = False
        try:
            with os.fdopen(handle, "wb") as stream:
                stream.write(self.writer_token.encode())
                stream.flush()
            self.locked = True
            yield self
        finally:
            self.locked = False
            if not self.retain_lock and self._owns_lock():
                self.lock_path.unlink(missing_ok=True)
            self.writer_token = None

    @contextmanager
    def borrowed_writer(self, token: str):
        """The supervisor retains the exclusive lock; this loan never unlocks it."""
        if self.locked or not isinstance(token, str) or len(token) != 32:
            raise Unavailable("invalid_writer_loan")
        self.writer_token = token
        try:
            if not self._owns_lock():
                raise Unavailable("writer_loan_mismatch")
            self.locked = True
            yield self
        finally:
            self.locked = False
            self.writer_token = None

    def load(self) -> tuple[Checkpoint | None, str]:
        try:
            if self.path.stat().st_size > 8 * 1024 * 1024:
                return None, "state_oversized"
            with self.path.open("rb") as stream:
                raw = stream.read(8 * 1024 * 1024 + 1)
            if len(raw) > 8 * 1024 * 1024:
                return None, "state_oversized"
            envelope = json.loads(raw)
            if envelope["sha256"] != _digest(envelope["state"]):
                return None, "state_digest_mismatch"
            return Checkpoint.decode(envelope["state"]), "state_hit"
        except FileNotFoundError:
            return None, "state_missing_notifications_blocked"
        except (ValueError, KeyError, TypeError, OSError, OverflowError) as exc:
            return None, "state_invalid_notifications_blocked:" + type(exc).__name__

    def save(self, state: Checkpoint, expected_generation: int | None):
        if not self.locked or not self._owns_lock():
            raise Unavailable("writer_lock_required")
        if expected_generation is not None and (type(expected_generation) is not int or expected_generation < 0):
            raise ValueError("Invalid expected generation")
        existing, reason = self.load()
        if expected_generation is None:
            # Missing is allowed for an explicitly non-notifying baseline only.
            if self.path.exists() or state.notifications_enabled or state.generation != 0:
                raise Unavailable("initialization_requires_missing_nonnotifying_state")
        elif existing is None or existing.generation != expected_generation or state.generation != expected_generation:
            raise Unavailable("stale_or_missing_generation:" + reason)
        previous = state.generation
        state.generation += 1
        try:
            raw = _checkpoint_bytes(state)
            if not self._owns_lock():
                raise Unavailable("writer_lock_changed")
            atomic_write(self.path, raw)
        except BaseException:
            state.generation = previous
            raise

    def restore_shadow(self, state: Checkpoint) -> Checkpoint:
        """Adopt into a fresh locked workspace only; never enable publication.

        Generation is mirrored, not incremented. Any existing state (including a
        corrupt file or dangling symlink) requires explicit reconciliation outside
        this operation. This is not distributed CAS or a baseline approval.
        """
        if not self.locked or not self._owns_lock():
            raise Unavailable("writer_lock_required")
        if os.path.lexists(self.path):
            raise Unavailable("shadow_restore_requires_fresh_workspace")
        if not isinstance(state, Checkpoint) or type(state.notifications_enabled) is not bool:
            raise ValueError("invalid_shadow_checkpoint")
        restored = deepcopy(state)
        restored.notifications_enabled = False
        raw = _checkpoint_bytes(restored)
        if not self._owns_lock() or os.path.lexists(self.path):
            raise Unavailable("shadow_restore_target_or_lock_changed")
        atomic_write(self.path, raw)
        return restored

    def enable_baseline(self, *, checkpoint_sha256: str, generation: int, at: int, products: tuple):
        """Explicit local approval of a complete non-notifying scan, not a send.

        Caller owns approval and the complete admitted product pool. Never reset
        unresolved intent, replay this scan's transitions, or infer remote save.
        """
        if not self.locked or not self._owns_lock():
            raise Unavailable('writer_lock_required')
        if (not isinstance(checkpoint_sha256, str) or len(checkpoint_sha256) != 64
                or any(c not in '0123456789abcdef' for c in checkpoint_sha256)
                or type(generation) is not int or generation < 1 or not _minute(at)
                or not isinstance(products, tuple) or not 1 <= len(products) <= 1000):
            raise ValueError('baseline_approval_identity')
        keys = tuple(_product_key(p) for p in products)
        if len(set(keys)) != len(keys):
            raise ValueError('baseline_duplicate_products')
        with self.path.open('rb') as stream:
            raw = stream.read(8 * 1024 * 1024 + 1)
        if hashlib.sha256(raw).hexdigest() != checkpoint_sha256:
            raise Unavailable('baseline_checkpoint_changed')
        state, reason = self.load()
        if (state is None or state.generation != generation or state.at != at or state.notifications_enabled
                or any(v['status'] != 'confirmed' for v in state.ledger.values())
                or any(state.tracker.last_at.get(('A0', key)) != at for key in keys)):
            raise Unavailable('baseline_incomplete_or_unresolved:' + reason)
        state.notifications_enabled = True
        self.save(state, generation)
        return state


def _checkpoint_bytes(state: Checkpoint) -> bytes:
    body = state.encode()
    Checkpoint.decode(body)
    raw = json.dumps({"state": body, "sha256": _digest(body)}, sort_keys=True,
                     separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > 8 * 1024 * 1024:
        raise Unavailable("state_byte_limit")
    return raw


def atomic_write(path: Path, data: bytes):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
