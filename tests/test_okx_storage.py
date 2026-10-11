"""Fake-carrier protocol tests. No GitHub, archives, messages or GB allocations."""
import json
from dataclasses import replace

import pytest

from crypto_boom import okx_storage as storage
from crypto_boom.okx_state import Checkpoint
from crypto_boom.okx_trial import HOUR, MINUTE, Transition

SCOPE = storage.Scope("nostalume/crypto-boom-alert", 123, "refs/heads/main")
FOREIGN = replace(SCOPE, workflow_id=456)
SOURCE = "a" * 40
T = 2000000000 * MINUTE


class FakeBackend:
    def __init__(self):
        self.records = {}
        self.bodies = {}
        self.complete = True
        self.usage_known = True
        self.foreign_cache = 0
        self.foreign_billable = 0
        self.upload_error = False
        self.unknown = False
        self.mismatch = False
        self.delete_fail = set()
        self.read_fail = set()
        self.deleted = []
        self.uploads = []
        self.peak = {"state": 0, "market": 0}
        self.retain_deleted_usage = 0
        self.lagging_usage = False
        self.post_inventory_fail = False
        self.cache_free_write = True
        self.artifact_free_write = True

    def inventory(self):
        if self.post_inventory_fail and self.uploads:
            raise OSError("post_upload_list_failed")
        objects = tuple(self.records.values())
        cache = self.foreign_cache + sum(x.size for x in objects if x.kind == "market")
        billable = self.foreign_billable + sum(x.size for x in objects if x.kind == "state")
        return storage.Inventory(objects, self.complete,
                                 cache if self.usage_known else None,
                                 billable + self.retain_deleted_usage if self.usage_known else None,
                                 self.cache_free_write, self.artifact_free_write)

    def read(self, object_id, limit):
        if object_id in self.read_fail:
            raise OSError("missing_blob")
        body = self.bodies[object_id]
        if len(body) > limit:
            raise ValueError("bounded_read")
        return body

    def upload(self, snapshot, body):
        assert snapshot.object_id not in self.records  # Immutable, never blind overwrite.
        self.uploads.append(snapshot.object_id)
        if self.upload_error:
            raise OSError("upload_failed")
        confirmed = replace(snapshot, confirmed=not self.unknown, validated=True)
        self.records[snapshot.object_id] = confirmed
        self.bodies[snapshot.object_id] = body
        self.peak[snapshot.kind] = max(self.peak[snapshot.kind], sum(
            x.size for x in self.records.values() if x.kind == snapshot.kind and x.scope == snapshot.scope))
        if self.unknown:
            return None
        return replace(confirmed, size=confirmed.size + 1) if self.mismatch else confirmed

    def delete(self, object_id):
        self.deleted.append(object_id)
        if object_id in self.delete_fail:
            return False
        removed = self.records.pop(object_id)
        self.bodies.pop(object_id)
        if self.lagging_usage and removed.kind == "state":
            self.retain_deleted_usage += removed.size
        return True


def query(now=T, receipts=(), complete=True, scope=SCOPE):
    return storage.ReceiptQuery(scope, now - 2 * storage.DAY, now, complete, receipts)


def bundle(generation=0, parent=None, *, at=T, enabled=False, ledger=None, receipts=None):
    checkpoint = Checkpoint(generation=generation, at=at, notifications_enabled=enabled,
                            ledger={} if ledger is None else ledger)
    return storage.pack_state(checkpoint, scope=SCOPE, source_sha=SOURCE, run_id=generation + 1,
                              attempt=1, created_at=at, parent=parent, receipts=receipts)


def candidate(raw, size=10 * storage.MIB, name=None):
    generation = json.loads(raw)["generation"]
    return storage.state_candidate(name or f"state-{generation}", raw, size)


def add(backend, raw, **kwargs):
    record = replace(candidate(raw, **kwargs), confirmed=True, validated=True)
    backend.records[record.object_id] = record
    backend.bodies[record.object_id] = raw
    return record


def chain(count=3, *, enabled=False):
    backend = FakeBackend()
    raw = None
    for generation in range(count):
        raw = bundle(generation, raw, at=T + generation * 10 * MINUTE, enabled=enabled and generation > 0)
        add(backend, raw)
    return backend, raw


def restore(backend, now=T + 20 * MINUTE, receipts=None, minimum_generation=0):
    return storage.recover_state(backend, scope=SCOPE, allowed_sources=frozenset({SOURCE}), now=now,
                                 receipts=query(now) if receipts is None else receipts,
                                 minimum_generation=minimum_generation)


def market(generation=0, at=T, *, size=600 * storage.MIB, scope=SCOPE):
    body = f"synthetic-market-{generation}".encode()
    record = storage.Snapshot(f"market-{generation}", "market", scope, SOURCE, generation + 1, 1,
                              generation, at, at, at + 7 * storage.DAY, storage.digest(body), size,
                              validated=True)
    return record, body


def pending(parent):
    checkpoint = Checkpoint(generation=1, at=T + 10 * MINUTE, notifications_enabled=True)
    identity = ("okx-alert-trial-v1", "A0", ("okx", "SPOT", "ALT-USDT"), "UP", checkpoint.at)
    transitions = [Transition(identity, "OBSERVED", checkpoint.at)]
    ids = checkpoint.prepare(transitions, checkpoint.at)
    raw = storage.pack_state(checkpoint, scope=SCOPE, source_sha=SOURCE, run_id=2, attempt=1,
                             created_at=checkpoint.at, parent=parent)
    return checkpoint, ids, raw


def test_disabled_seed_and_exact_round_trip():
    backend, raw = chain(2, enabled=True)
    result = restore(backend, now=T + 10 * MINUTE)
    assert result.reason == "restored" and result.may_prepare
    assert result.checkpoint.encode() == json.loads(raw)["state"]
    assert backend.bodies["state-1"] == raw  # Restoring never overwrites carrier data.
    assert not restore(chain(1)[0], now=T).may_prepare


@pytest.mark.parametrize("mutation", ["corrupt", "read_missing", "expired", "unconfirmed", "wrong_source"])
def test_bad_latest_never_restores_older_notifying_state(mutation):
    backend, _ = chain(3, enabled=True)
    record = backend.records["state-2"]
    now = T + 20 * MINUTE
    if mutation == "corrupt":
        backend.bodies["state-2"] = b"{}"
    elif mutation == "read_missing":
        backend.read_fail.add("state-2")
    elif mutation == "expired":
        now = record.expires_at
    elif mutation == "unconfirmed":
        backend.records["state-2"] = replace(record, confirmed=False)
    else:
        backend.records["state-2"] = replace(record, source_sha="b" * 40)
    result = restore(backend, now=now)
    assert result.checkpoint is None and not result.may_prepare


def test_missing_frontier_and_parent_and_conflict_close_gate():
    backend, _ = chain(2, enabled=True)
    assert restore(backend, minimum_generation=2).reason == "state_frontier_missing"
    backend.records.pop("state-0")
    assert restore(backend).reason == "state_parent_missing_or_conflicting"
    backend, _ = chain(3)
    backend.records["evil"] = replace(backend.records["state-2"], object_id="evil", sha256="b" * 64)
    assert restore(backend).reason == "state_frontier_conflict"
    backend, _ = chain(3)
    backend.records["state-2"] = replace(backend.records["state-2"], parent_digest="b" * 64)
    assert restore(backend).reason == "state_invalid:ValueError"
    backend, _ = chain(3)
    backend.records["state-1"] = replace(backend.records["state-1"], sha256="b" * 64)
    assert restore(backend).reason == "state_parent_mismatch"


def test_no_state_does_not_invent_notifying_baseline_from_cache():
    backend = FakeBackend()
    record, body = market()
    backend.records[record.object_id] = replace(record, confirmed=True)
    backend.bodies[record.object_id] = body
    assert restore(backend).reason == "state_missing"


@pytest.mark.parametrize("change", ["scope", "source_sha", "schema", "rules_version", "extra", "state_digest", "boolean_generation"])
def test_bundle_rejects_wrong_wire(change):
    body = json.loads(bundle())
    if change == "extra":
        body["unknown"] = 1
    elif change == "state_digest":
        body["state_sha256"] = "0" * 64
    elif change == "boolean_generation":
        body["generation"] = True
    elif change == "scope":
        body["scope"]["workflow_id"] = False
    else:
        body[change] = "unsupported"
    with pytest.raises((ValueError, TypeError)):
        storage.state_candidate("x", json.dumps(body).encode(), 10 * storage.MIB)


def test_duplicate_keys_and_byte_limit_are_rejected():
    raw = bundle()
    with pytest.raises(ValueError, match="duplicate_json_key"):
        storage.state_candidate("x", b'{"schema":"x",' + raw[1:], 10 * storage.MIB)
    with pytest.raises(ValueError, match="bundle_size"):
        storage.state_candidate("x", b" " * (storage.BUNDLE_LIMIT + 1), 10 * storage.MIB)
    with pytest.raises(ValueError, match="invalid_billable_size"):
        candidate(raw, size=0)
    # Provider ZIP can be smaller than the expanded JSON; these are separate limits.
    assert candidate(raw, size=len(raw) - 1).size == len(raw) - 1


@pytest.mark.parametrize("generation,enabled", [(0, True), (1, False)])
def test_seed_cannot_silently_enable_or_skip_generation(generation, enabled):
    with pytest.raises(ValueError, match="explicit_disabled_baseline"):
        bundle(generation, enabled=enabled)


def test_pending_receipt_restores_copy_only():
    backend, parent = chain(1)
    checkpoint, ids, raw = pending(parent)
    add(backend, raw)
    now = checkpoint.at
    unresolved = restore(backend, now)
    assert unresolved.reason == "publication_unresolved" and not unresolved.may_prepare
    receipt = storage.Receipt(SCOPE, 1, now, ids, checkpoint.ledger[ids[0]]["batch"])
    result = restore(backend, now, query(now, (receipt,)))
    assert result.may_prepare and result.checkpoint.ledger[ids[0]]["status"] == "confirmed"
    assert json.loads(backend.bodies["state-1"])["state"]["ledger"][ids[0]]["status"] == "pending"
    assert checkpoint.ledger[ids[0]]["status"] == "pending"


@pytest.mark.parametrize("mode", ["incomplete", "short_window", "future_generation", "unknown_id", "before_intent", "wrong_scope"])
def test_receipt_uncertainty_always_closes_gate(mode):
    backend, parent = chain(1)
    checkpoint, ids, raw = pending(parent)
    add(backend, raw)
    now = checkpoint.at
    receipt = storage.Receipt(SCOPE, 1, now, ids, checkpoint.ledger[ids[0]]["batch"])
    q = query(now, (receipt,))
    if mode == "incomplete":
        q = replace(q, complete=False)
    elif mode == "short_window":
        q = replace(q, start=now - HOUR)
    elif mode == "future_generation":
        q = replace(q, receipts=(replace(receipt, generation=2),))
    elif mode == "unknown_id":
        other_ids = ("b" * 64,)
        q = replace(q, receipts=(replace(receipt, ids=other_ids, batch=storage.digest(json.dumps(
            other_ids, separators=(",", ":")).encode())),))
    elif mode == "before_intent":
        q = replace(q, receipts=(replace(receipt, published_at=now - MINUTE),))
    else:
        q = query(now, (), scope=FOREIGN)
    result = restore(backend, now, q)
    assert result.checkpoint is not None and not result.may_prepare
    assert not result.checkpoint.notifications_enabled


def test_pending_must_survive_successor_and_confirmation_needs_proof():
    _, parent = chain(1)
    checkpoint, ids, raw = pending(parent)
    with pytest.raises(ValueError, match="lost_ledger_identity"):
        bundle(2, raw, at=T + 20 * MINUTE, enabled=True)
    next_state = Checkpoint.decode(json.loads(raw)["state"])
    next_state.generation = 2
    next_state.at += 10 * MINUTE
    next_state.reconcile(ids, checkpoint.ledger[ids[0]]["batch"])
    params = dict(scope=SCOPE, source_sha=SOURCE, run_id=3, attempt=1,
                  created_at=next_state.at, parent=raw)
    with pytest.raises(ValueError, match="promotion_requires_complete_receipts"):
        storage.pack_state(next_state, **params)
    with pytest.raises(ValueError, match="promotion_without_receipt"):
        storage.pack_state(next_state, receipts=query(next_state.at), **params)
    receipt = storage.Receipt(SCOPE, 1, checkpoint.at, ids, checkpoint.ledger[ids[0]]["batch"])
    promoted = storage.pack_state(next_state, receipts=query(next_state.at, (receipt,)), **params)
    backend = FakeBackend()
    add(backend, raw)
    add(backend, promoted)
    assert not restore(backend, next_state.at).may_prepare
    assert restore(backend, next_state.at, query(next_state.at, (receipt,))).may_prepare


def test_partial_receipt_cannot_confirm_batch():
    backend, parent = chain(1)
    state = Checkpoint(generation=1, at=T + 10 * MINUTE, notifications_enabled=True)
    changes = [Transition(("okx-alert-trial-v1", "A0", ("okx", "SPOT", f"ALT{i}-USDT"), "UP", state.at),
                          "OBSERVED", state.at) for i in range(2)]
    ids = state.prepare(changes, state.at)
    raw = storage.pack_state(state, scope=SCOPE, source_sha=SOURCE, run_id=2, attempt=1,
                             created_at=state.at, parent=parent)
    add(backend, raw)
    partial_ids = ids[:1]
    partial = storage.Receipt(SCOPE, 1, state.at, partial_ids,
                              storage.digest(json.dumps(partial_ids, separators=(",", ":")).encode()))
    result = restore(backend, state.at, query(state.at, (partial,)))
    assert not result.may_prepare and result.reason.startswith("receipts_invalid")


@pytest.mark.parametrize("kind", ["state", "market"])
def test_day_worst_case_capacity_is_bounded_metadata_only(kind):
    backend = FakeBackend()
    parent = None
    count, interval = (144, 10 * MINUTE) if kind == "state" else (24, HOUR)
    for generation in range(count):
        now = T + generation * interval
        if kind == "state":
            raw = bundle(generation, parent, at=now)
            record = candidate(raw)
            parent = raw
        else:
            record, raw = market(generation, now)
        result = storage.save_snapshot(backend, record, raw, now=now)
        assert result.reason == "saved" and result.confirmed is not None
        assert len(backend.records) <= 2
    expected_peak = 30 * storage.MIB if kind == "state" else 1800 * storage.MIB
    assert backend.peak[kind] == expected_peak
    assert len(backend.uploads) == count and len(backend.deleted) == count - 2
    assert sum(len(raw) for raw in backend.bodies.values()) < 10000


@pytest.mark.parametrize("bootstrap,minutes,expected", [(False, 10, "market_cadence"),
                                                           (False, 60, "saved"),
                                                           (True, 9, "market_cadence"),
                                                           (True, 10, "saved")])
def test_market_cadence_preserves_partial_warmup(bootstrap, minutes, expected):
    backend = FakeBackend()
    record, raw = market()
    assert storage.save_snapshot(backend, record, raw, now=T).reason == "saved"
    now = T + minutes * MINUTE
    record, raw = market(1, now)
    assert storage.save_snapshot(backend, record, raw, now=now, bootstrap=bootstrap).reason == expected


@pytest.mark.parametrize("scenario,reason", [("unknown", "usage_unknown"), ("incomplete", "inventory_incomplete"),
                                              ("account_full", "shared_capacity"), ("object_large", "object_capacity")])
def test_free_quota_refusal_does_not_upload(scenario, reason):
    backend = FakeBackend()
    record, raw = candidate(bundle()), bundle()
    if scenario == "unknown":
        backend.usage_known = False
    elif scenario == "incomplete":
        backend.complete = False
    elif scenario == "account_full":
        backend.foreign_billable = 375 * storage.MIB
    else:
        record = replace(record, size=11 * storage.MIB)
    result = storage.save_snapshot(backend, record, raw, now=T)
    assert result.reason == reason and not backend.uploads


def test_lagging_usage_does_not_credit_confirmed_or_planned_deletion():
    backend, parent = chain(3)
    backend.foreign_billable = 355 * storage.MIB
    backend.lagging_usage = True
    raw = bundle(3, parent, at=T + 30 * MINUTE)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE)
    assert result.reason == "shared_capacity" and not backend.uploads
    assert set(backend.records) == {"state-1", "state-2"}
    assert backend.inventory().billable_usage == 385 * storage.MIB


@pytest.mark.parametrize("failure", ["upload_error", "unknown", "mismatch"])
def test_failed_upload_preserves_latest_two_and_never_retries(failure):
    backend, parent = chain(3)
    raw = bundle(3, parent, at=T + 30 * MINUTE)
    setattr(backend, failure, True)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE)
    assert result.confirmed is None and {"state-1", "state-2"} <= set(backend.records)
    assert backend.uploads == ["state-3"]
    assert "state-1" not in backend.deleted and "state-2" not in backend.deleted
    if failure == "unknown":
        again = storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE)
        assert again.reason == "upload_requires_reconciliation" and backend.uploads == ["state-3"]
        backend.records["state-3"] = replace(backend.records["state-3"], confirmed=True)
        assert storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE).reason == "already_confirmed"


def test_success_with_cleanup_failure_retains_true_new_state():
    backend, parent = chain(2)
    backend.delete_fail.add("state-0")
    raw = bundle(2, parent, at=T + 20 * MINUTE)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 20 * MINUTE)
    assert result.reason == "saved_degraded" and result.confirmed.object_id == "state-2"
    assert result.cleanup_failed == ("state-0",) and len(backend.records) == 3
    backend.post_inventory_fail = True
    # A fresh backend demonstrates successful upload / unavailable post-list.
    other, parent = chain(2)
    other.post_inventory_fail = True
    raw = bundle(2, parent, at=T + 20 * MINUTE)
    result = storage.save_snapshot(other, candidate(raw), raw, now=T + 20 * MINUTE)
    assert result.reason == "saved_cleanup_unavailable" and result.confirmed is not None
    assert "state-2" in other.records


def test_foreign_and_unvalidated_objects_are_never_deleted():
    backend, parent = chain(3)
    backend.records["foreign"] = replace(backend.records["state-0"], object_id="foreign", scope=FOREIGN)
    backend.bodies["foreign"] = b"not-ours"
    original = backend.bodies["state-0"]
    backend.bodies["state-0"] = b"corrupt-old"
    raw = bundle(3, parent, at=T + 30 * MINUTE)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE)
    assert result.confirmed is None and result.reason == "retention_backlog"
    assert "foreign" not in backend.deleted and "state-0" not in backend.deleted
    assert {"foreign", "state-0"} <= set(backend.records)
    backend.bodies["state-0"] = original
    assert storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE).confirmed is not None
    assert "foreign" not in backend.deleted and "foreign" in backend.records


def test_identity_collision_stale_generation_and_parent_mismatch_refuse():
    backend, parent = chain(2)
    raw = bundle(2, parent, at=T + 20 * MINUTE)
    record = candidate(raw, name="state-1")
    assert storage.save_snapshot(backend, record, raw, now=T + 20 * MINUTE).reason == "identity_collision"
    old = backend.bodies["state-0"]
    assert storage.save_snapshot(backend, candidate(old, name="stale"), old, now=T).reason == "stale_or_conflicting_frontier"
    body = json.loads(raw)
    body["parent_digest"] = "b" * 64
    raw = json.dumps(body).encode()
    assert storage.save_snapshot(backend, candidate(raw), raw, now=T + 20 * MINUTE).reason == "parent_mismatch"
    assert not backend.uploads


def test_market_miss_expiration_and_validation_are_rebuildable():
    backend = FakeBackend()
    record, raw = market()
    assert storage.save_snapshot(backend, replace(record, validated=False), raw, now=T).reason == "market_validation_required"
    assert storage.save_snapshot(backend, record, raw, now=T).confirmed is not None
    assert storage.market_candidate(backend.inventory(), SCOPE, T) is not None
    assert storage.market_candidate(backend.inventory(), SCOPE, record.expires_at) is None
    assert storage.market_candidate(backend.inventory(), FOREIGN, T) is None


def test_inventory_and_receipt_bounds():
    with pytest.raises(ValueError, match="invalid_inventory"):
        storage.Inventory(tuple(candidate(bundle(), name=f"x-{i}") for i in range(1001)), True, 0, 20 * storage.GIB)
    with pytest.raises(ValueError, match="underreported_usage"):
        storage.Inventory((candidate(bundle()),), True, 0, 0)
    ids = ("a" * 64,)
    receipt = storage.Receipt(SCOPE, 0, T, ids, storage.digest(json.dumps(ids, separators=(",", ":")).encode()))
    with pytest.raises(ValueError, match="invalid_receipt_query"):
        query(T, (receipt,) * 1001)
    with pytest.raises(ValueError, match="invalid_snapshot"):
        replace(candidate(bundle()), object_id="../unsafe")


def test_cleanup_backlog_cannot_create_a_fourth_version():
    backend, parent = chain(3)
    backend.delete_fail.add("state-0")
    raw = bundle(3, parent, at=T + 30 * MINUTE)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE)
    assert result.reason == "retention_backlog" and not backend.uploads
    assert len(backend.records) == 3


def test_project_and_repository_peak_limits_include_all_matching_usage():
    backend = FakeBackend()
    record, raw = market()
    backend.foreign_cache = 8 * storage.GIB - record.size + 1
    assert storage.save_snapshot(backend, record, raw, now=T).reason == "shared_capacity"
    # Pure capacity accounting covers excess already-present project objects,
    # regardless of whether a later retention cleanup could remove any of them.
    records = tuple(replace(record, object_id=f"x-{i}", generation=i) for i in range(4))
    inventory = storage.Inventory(records, True, 4 * record.size, 0, True, True)
    assert storage.quota_reason(inventory, record) == "project_capacity"
    large = replace(record, size=600 * storage.MIB + 1)
    assert storage.quota_reason(backend.inventory(), large) == "object_capacity"
    state_records = tuple(replace(candidate(bundle()), object_id=f"s-{i}") for i in range(6))
    inventory = storage.Inventory(state_records, True, 0, 60 * storage.MIB, True, True)
    assert storage.quota_reason(inventory, candidate(bundle())) == "project_capacity"


def test_changed_frontier_after_cleanup_is_fenced_before_upload():
    backend, parent = chain(2)
    raw = bundle(2, parent, at=T + 20 * MINUTE)
    original = backend.inventory
    calls = 0
    def changed():
        nonlocal calls
        calls += 1
        if calls == 2:
            add(backend, raw, name="other-writer-2")
        return original()
    backend.inventory = changed
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 20 * MINUTE)
    assert result.reason == "frontier_changed" and not backend.uploads


def test_missing_existing_parent_blocks_successor_upload():
    backend, parent = chain(2)
    backend.records.pop("state-0")
    raw = bundle(2, parent, at=T + 20 * MINUTE)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 20 * MINUTE)
    assert result.reason == "state_parent_missing_or_conflicting" and not backend.uploads


@pytest.mark.parametrize("status", ["pending", "unknown"])
def test_unresolved_intent_never_ages_out_of_retained_snapshots(status):
    _, parent = chain(1)
    checkpoint, ids, raw = pending(parent)
    checkpoint.ledger[ids[0]]["status"] = status
    raw = storage.pack_state(checkpoint, scope=SCOPE, source_sha=SOURCE, run_id=2, attempt=1,
                             created_at=checkpoint.at, parent=parent)
    later = checkpoint.at + 3 * storage.DAY
    with pytest.raises(ValueError, match="lost_ledger_identity"):
        bundle(2, raw, at=later, enabled=True)
    kept = bundle(2, raw, at=later, enabled=True, ledger=checkpoint.ledger)
    backend = FakeBackend()
    add(backend, raw)
    add(backend, kept)
    result = restore(backend, later)
    assert result.reason == "publication_unresolved" and ids[0] in result.checkpoint.ledger


def test_partial_receipt_query_disables_even_empty_enabled_state():
    backend, _ = chain(2, enabled=True)
    result = restore(backend, T + 10 * MINUTE, query(T + 10 * MINUTE, complete=False))
    assert result.checkpoint is not None and result.reason == "receipts_incomplete"
    assert not result.may_prepare and not result.checkpoint.notifications_enabled
    backend.complete = False
    assert restore(backend).reason == "inventory_incomplete"


def test_aggregate_receipt_message_bound():
    ids = tuple(f"{i:064x}" for i in range(1000))
    batch = storage.digest(json.dumps(ids, separators=(",", ":")).encode())
    receipt = storage.Receipt(SCOPE, 0, T, ids, batch)
    with pytest.raises(ValueError, match="invalid_receipt_query"):
        query(T, (receipt,) * 11)


def test_confirmed_identity_is_preserved_until_48h_and_cannot_regress():
    _, parent = chain(1)
    checkpoint, ids, raw = pending(parent)
    receipt = storage.Receipt(SCOPE, 1, checkpoint.at, ids, checkpoint.ledger[ids[0]]["batch"])
    checkpoint.generation = 2
    checkpoint.at += 10 * MINUTE
    checkpoint.reconcile(ids, receipt.batch)
    promoted = bundle(2, raw, at=checkpoint.at, enabled=True, ledger=checkpoint.ledger,
                       receipts=query(checkpoint.at, (receipt,)))
    with pytest.raises(ValueError, match="lost_ledger_identity"):
        bundle(3, promoted, at=checkpoint.at + MINUTE, enabled=True)
    regressed = json.loads(promoted)["state"]["ledger"]
    regressed[ids[0]]["status"] = "pending"
    with pytest.raises(ValueError, match="ledger_regression"):
        bundle(3, promoted, at=checkpoint.at + MINUTE, enabled=True, ledger=regressed)
    aged_out = bundle(3, promoted, at=checkpoint.at + 3 * storage.DAY, enabled=True)
    assert not json.loads(aged_out)["state"]["ledger"]


def test_source_allowlist_and_carrier_scope_must_match_both_generations():
    backend, _ = chain(2, enabled=True)
    backend.records["state-0"] = replace(backend.records["state-0"], source_sha="b" * 40)
    assert restore(backend).reason == "state_parent_mismatch"
    backend, _ = chain(2, enabled=True)
    backend.records["state-1"] = replace(backend.records["state-1"], scope=FOREIGN)
    assert not restore(backend, minimum_generation=1).may_prepare
    assert restore(backend, minimum_generation=1).reason == "state_frontier_missing"


def test_unconfirmed_unknown_write_blocks_new_successor():
    backend, parent = chain(2)
    raw = bundle(2, parent, at=T + 20 * MINUTE)
    backend.unknown = True
    assert storage.save_snapshot(backend, candidate(raw), raw, now=T + 20 * MINUTE).reason == "upload_unknown"
    next_raw = bundle(3, raw, at=T + 30 * MINUTE)
    assert storage.save_snapshot(backend, candidate(next_raw), next_raw, now=T + 30 * MINUTE).reason == "stale_or_conflicting_frontier"
    assert backend.uploads == ["state-2"] and not backend.deleted


def test_retention_conflict_and_bad_protected_content_never_delete_older_evidence():
    backend, parent = chain(3)
    backend.records["conflict"] = replace(backend.records["state-2"], object_id="conflict", sha256="f" * 64)
    raw = bundle(3, parent, at=T + 30 * MINUTE)
    assert storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE).reason == "stale_or_conflicting_frontier"
    assert not backend.deleted and not backend.uploads
    backend, parent = chain(3)
    backend.bodies["state-1"] = b"bad-parent"
    raw = bundle(3, parent, at=T + 30 * MINUTE)
    assert storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE).confirmed is None
    assert not backend.deleted and not backend.uploads


def test_unknown_receipt_outside_promotions_is_not_ignored():
    backend, raw = chain(2, enabled=True)
    ids = ("e" * 64,)
    receipt = storage.Receipt(SCOPE, 1, T + 10 * MINUTE, ids,
                              storage.digest(json.dumps(ids, separators=(",", ":")).encode()))
    with pytest.raises(ValueError, match="receipt_missing_intent"):
        bundle(2, raw, at=T + 20 * MINUTE, enabled=True, receipts=query(T + 20 * MINUTE, (receipt,)))


def test_recovery_rejects_oversized_provider_object_even_with_small_json():
    backend, _ = chain(2)
    backend.records["state-1"] = replace(backend.records["state-1"], size=11 * storage.MIB)
    assert restore(backend).reason == "state_invalid:ValueError"


@pytest.mark.parametrize("kind,allowed,reason", [("state", None, "free_write_unknown"),
                                                ("state", False, "free_write_denied"),
                                                ("market", None, "free_write_unknown"),
                                                ("market", False, "free_write_denied")])
def test_low_current_usage_cannot_replace_free_billing_admission(kind, allowed, reason):
    backend = FakeBackend()
    if kind == "state":
        backend.artifact_free_write = allowed
        raw = bundle()
        record = candidate(raw)
    else:
        backend.cache_free_write = allowed
        record, raw = market()
    result = storage.save_snapshot(backend, record, raw, now=T)
    assert result.reason == reason and not backend.uploads


def test_deletion_never_erases_accumulated_billing_denial():
    backend, parent = chain(3)
    backend.artifact_free_write = False  # Historical GB-hours or budget denial.
    raw = bundle(3, parent, at=T + 30 * MINUTE)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 30 * MINUTE)
    assert result.reason == "free_write_denied"
    assert backend.deleted == ["state-0"] and len(backend.records) == 2
    assert not backend.uploads and backend.inventory().billable_usage == 20 * storage.MIB


def test_expired_frontier_cannot_be_resaved_as_fresh_notification_state():
    backend, parent = chain(2, enabled=True)
    now = T + 3 * storage.DAY
    raw = bundle(2, parent, at=now, enabled=True)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=now)
    assert result.reason == "state_frontier_expired" and not backend.uploads and not backend.deleted


def test_expired_shorter_provider_lifetime_is_not_an_idempotent_success():
    backend, raw = chain(1)
    original = backend.records["state-0"]
    backend.records["state-0"] = replace(original, expires_at=T + MINUTE)
    result = storage.save_snapshot(backend, candidate(raw), raw, now=T + 2 * MINUTE)
    assert result.reason == "existing_expired" and result.confirmed is None and not backend.uploads


def test_compressed_provider_size_does_not_limit_expanded_state_wire():
    backend = FakeBackend()
    raw = bundle()
    record = candidate(raw, size=len(raw) // 2)
    assert storage.save_snapshot(backend, record, raw, now=T).reason == "saved"
    assert restore(backend, now=T).checkpoint is not None
