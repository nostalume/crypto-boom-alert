"""Offline provider contracts + writer-controlled shadow adoption, no remote effects."""
import io
import json
import stat
import zipfile
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone

import httpx
import pytest

from crypto_boom import okx_github as github
from crypto_boom import okx_state as state
from crypto_boom import okx_storage as storage
from crypto_boom.okx_trial import MINUTE, VERSION, Transition, Unavailable

SCOPE = storage.Scope("nostalume/crypto-boom-alert", 123, "refs/heads/main")
SOURCE = "a" * 40
T = 1780000020000


def iso(value):
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def zip_body(body, *, name="state-bundle.json", mode=stat.S_IFREG | 0o600, method=zipfile.ZIP_DEFLATED,
             duplicate=False, extra=b""):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        entry = zipfile.ZipInfo(name)
        entry.external_attr = mode << 16
        entry.compress_type = method
        entry.extra = extra
        archive.writestr(entry, body)
        if duplicate:
            archive.writestr("duplicate", body)
    return stream.getvalue()


def response(body, status=200, headers=None):
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    return httpx.Response(status, headers=headers, stream=httpx.ByteStream(raw))


class Provider:
    def __init__(self, count=2):
        self.repo = {"id": 1391009742, "full_name": SCOPE.repository, "default_branch": "main"}
        self.items, self.runs, self.archives, self.raws = [], {}, {}, {}
        self.requests = []
        self.list_calls = 0
        self.repo_calls = 0
        self.list_hook = None
        self.repo_hook = None
        self.redirect = None
        self.blob_status = 200
        self.api_status = 200
        self.encoding = None
        parent = None
        for generation in range(count):
            checkpoint = state.Checkpoint(generation=generation, at=T + generation * 10 * MINUTE,
                                          notifications_enabled=generation > 0)
            raw = storage.pack_state(checkpoint, scope=SCOPE, source_sha=SOURCE, run_id=generation + 1,
                                     attempt=1, created_at=checkpoint.at, parent=parent)
            archive = zip_body(raw)
            artifact_id, run_id = 100 + generation, generation + 1
            self.runs[run_id] = {"id": run_id, "workflow_id": SCOPE.workflow_id, "event": "schedule",
                                 "run_attempt": 1, "head_branch": "main", "head_sha": SOURCE,
                                 "repository": {"id": self.repo["id"]}, "head_repository": {"id": self.repo["id"]},
                                 "pull_requests": [], "run_started_at": iso(checkpoint.at - MINUTE),
                                 "status": "completed", "conclusion": "failure"}
            self.items.append({"id": artifact_id, "name": f"okx-state-v1-g{generation}-a1-{storage.digest(raw)}",
                               "size_in_bytes": len(archive), "created_at": iso(checkpoint.at),
                               "expires_at": iso(checkpoint.at + storage.DAY), "expired": False,
                               "digest": "sha256:" + storage.digest(archive),
                               "workflow_run": {"id": run_id, "repository_id": self.repo["id"],
                                                "head_repository_id": self.repo["id"], "head_branch": "main",
                                                "head_sha": SOURCE}})
            self.archives[artifact_id] = archive
            self.raws[artifact_id] = raw
            parent = raw

    def handle(self, request):
        self.requests.append(request)
        path = request.url.path
        if request.url.host != "api.github.com":
            assert "Authorization" not in request.headers and "Cookie" not in request.headers
            artifact_id = int(path.rsplit("/", 1)[1])
            headers = {"Set-Cookie": "forbidden=1; Path=/"}
            if self.encoding:
                headers["Content-Encoding"] = self.encoding
            return response(self.archives[artifact_id], self.blob_status, headers)
        if self.api_status != 200:
            return response(b"error", self.api_status)
        if path.endswith("/zip"):
            artifact_id = int(path.split("/")[-2])
            return response(b"", 302, {"Location": self.redirect or
                            f"https://results.blob.core.windows.net/files/{artifact_id}?sig=unit-test"})
        if path == f"/repos/{SCOPE.repository}":
            self.repo_calls += 1
            if self.repo_hook:
                self.repo_hook(self)
            return response(self.repo)
        if path.endswith("/artifacts"):
            self.list_calls += 1
            if self.list_hook:
                self.list_hook(self)
            page = int(request.url.params["page"])
            return response({"total_count": len(self.items), "artifacts": self.items[(page - 1) * 100:page * 100]})
        parts = path.split("/")
        if "runs" in parts:
            run_id = int(parts[parts.index("runs") + 1])
            if "attempts" in parts:
                return response(self.runs[(run_id, int(parts[-1]))])
            return response(self.runs[run_id])
        raise AssertionError(path)

    def reader(self, **kwargs):
        return github.GitHubStateReader(SCOPE, frozenset({SOURCE}), T + 20 * MINUTE,
                                       token="unit-test-token", transport=httpx.MockTransport(self.handle), **kwargs)


def recover(reader):
    return storage.recover_state(reader, scope=SCOPE, allowed_sources=frozenset({SOURCE}),
                                 now=T + 20 * MINUTE, minimum_generation=1)


def test_provider_to_shadow_store_preserves_generation_and_blocks_notifications(tmp_path):
    provider = Provider()
    with provider.reader() as reader:
        restored = recover(reader)
        assert restored.checkpoint is not None and restored.reason == "receipts_incomplete"
        assert not restored.may_prepare and not restored.checkpoint.notifications_enabled
        assert reader.requests <= 32 and reader.received < 32 * storage.MIB
        assert restored.snapshot.size < len(provider.raws[101])  # Real compressed ZIP, not JSON size.
        store = state.StateStore(tmp_path / "checkpoint.json")
        with store.writer():
            adopted = store.restore_shadow(restored.checkpoint)
        loaded, reason = store.load()
        assert reason == "state_hit" and loaded.encode() == adopted.encode() == restored.checkpoint.encode()
        assert loaded.generation == 1 and not store.lock_path.exists()
        with store.writer():
            store.save(loaded, 1)
        assert loaded.generation == 2
        assert all(r.method == "GET" for r in provider.requests)
        assert all(r.headers.get("Authorization") == "Bearer unit-test-token"
                   for r in provider.requests if r.url.host == "api.github.com")
        with pytest.raises(PermissionError):
            reader.upload(restored.snapshot, provider.raws[101])
        with pytest.raises(PermissionError):
            reader.delete("101")
    assert not reader.bodies
    with pytest.raises(OSError, match="closed"):
        reader.inventory()


def test_foreign_prefix_and_workflow_do_not_become_owned_objects():
    provider = Provider()
    foreign = deepcopy(provider.items[0])
    foreign["id"] = 999
    foreign["name"] = "okx-state-v1-malformed-but-foreign"
    foreign["workflow_run"]["id"] = 999
    provider.runs[999] = {"workflow_id": 456}
    provider.items.append(foreign)
    with provider.reader() as reader:
        inventory = reader.inventory()
        assert len(inventory.objects) == 2 and inventory.complete
        assert inventory.billable_usage is None and inventory.artifact_free_write is None
        assert not any("artifacts/999/zip" in r.url.path for r in provider.requests)


@pytest.mark.parametrize("field,value", [("head_sha", "b" * 40), ("head_branch", "feature"),
                                         ("head_repository", {"id": 99}), ("repository", None),
                                         ("event", "pull_request"), ("pull_requests", [{"number": 1}]),
                                         ("run_attempt", True), ("workflow_id", True)])
def test_untrusted_producer_is_never_downloaded(field, value):
    provider = Provider()
    provider.runs[2][field] = value
    with provider.reader() as reader:
        result = recover(reader)
        assert result.checkpoint is None and not result.may_prepare
        assert not any("artifacts/101/zip" in r.url.path for r in provider.requests)


def test_provider_attempt_is_bound_to_the_correct_historical_run():
    provider = Provider()
    original = deepcopy(provider.runs[2])
    provider.runs[2]["run_attempt"] = 2
    provider.runs[(2, 1)] = original
    with provider.reader() as reader:
        assert recover(reader).checkpoint is not None
        assert any(r.url.path.endswith("runs/2/attempts/1") for r in provider.requests)
    provider.runs[(2, 1)]["head_sha"] = "b" * 40
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None


@pytest.mark.parametrize("mutation", ["name_generation", "name_hash", "digest", "expired", "oversized", "long_retention", "summary"])
def test_latest_artifact_failure_cannot_fall_back(mutation):
    provider = Provider()
    item = provider.items[-1]
    if mutation == "name_generation":
        item["name"] = item["name"].replace("-g1-", "-g9-")
    elif mutation == "name_hash":
        item["name"] = item["name"][:-64] + "b" * 64
    elif mutation == "digest":
        item["digest"] = "sha256:" + "b" * 64
    elif mutation == "expired":
        item["expired"] = True
    elif mutation == "oversized":
        item["size_in_bytes"] = 10 * storage.MIB + 1
    elif mutation == "long_retention":
        item["expires_at"] = iso(T + 90 * storage.DAY)
    else:
        item["workflow_run"]["head_branch"] = "other"
    with provider.reader() as reader:
        result = recover(reader)
        assert result.checkpoint is None and not result.may_prepare


def test_parent_conflict_and_trusted_frontier_are_still_enforced():
    provider = Provider()
    with provider.reader() as reader:
        assert storage.recover_state(reader, scope=SCOPE, allowed_sources=frozenset({SOURCE}),
                                     now=T + 20 * MINUTE, minimum_generation=2).reason == "state_frontier_missing"
    raw = json.loads(provider.raws[101])
    raw["parent_digest"] = "c" * 64
    body = json.dumps(raw).encode()
    archive = zip_body(body)
    provider.items[1].update(name=f"okx-state-v1-g1-a1-{storage.digest(body)}", size_in_bytes=len(archive),
                             digest="sha256:" + storage.digest(archive))
    provider.archives[101] = archive
    with provider.reader() as reader:
        assert recover(reader).reason == "state_parent_mismatch"


@pytest.mark.parametrize("url", ["http://results.blob.core.windows.net/files/101", "https://localhost/files/101",
                                 "https://blob.core.windows.net.evil.example/files/101",
                                 "https://user:password@results.blob.core.windows.net/files/101",
                                 "https://results.blob.core.windows.net:8443/files/101",
                                 "https://results.blob.core.windows.net/files/101#fragment"])
def test_redirect_cannot_exfiltrate_credentials_or_reach_unapproved_hosts(url):
    provider = Provider()
    provider.redirect = url
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None
        assert all(r.url.host == "api.github.com" for r in provider.requests)


@pytest.mark.parametrize("status", [401, 403, 410, 429, 500])
def test_http_failure_is_bounded_and_not_retried(status):
    provider = Provider()
    provider.api_status = status
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None and reader.requests == 1
    provider = Provider()
    provider.blob_status = status
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None
        assert len([r for r in provider.requests if r.url.host != "api.github.com"]) == 1


def test_encoded_body_second_redirect_and_byte_deadline_limits_fail_closed():
    provider = Provider()
    provider.encoding = "gzip"
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None
    provider = Provider()
    provider.blob_status = 302
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None
        assert sum(r.url.host != "api.github.com" for r in provider.requests) == 1
    provider = Provider()
    with provider.reader() as reader:
        reader.requests = 32
        assert recover(reader).checkpoint is None and not provider.requests
    now = [0.0]
    with provider.reader(clock=lambda: now[0]) as reader:
        now[0] = 120.0
        assert recover(reader).checkpoint is None


def test_response_byte_guards_apply_while_streaming():
    transport = httpx.MockTransport(lambda r: response(b" " * (2 * storage.MIB + 1)))
    with github.GitHubStateReader(SCOPE, frozenset({SOURCE}), T, transport=transport) as reader:
        assert recover(reader).checkpoint is None and reader.received <= 3 * storage.MIB
    transport = httpx.MockTransport(lambda r: response(b"x", headers={"Content-Length": str(3 * storage.MIB)}))
    with github.GitHubStateReader(SCOPE, frozenset({SOURCE}), T, transport=transport) as reader:
        assert recover(reader).checkpoint is None and reader.received == 0


def test_aggregate_reader_bytes_are_bounded():
    provider = Provider()
    with provider.reader() as reader:
        reader.received = 32 * storage.MIB
        assert recover(reader).checkpoint is None
        assert 32 * storage.MIB < reader.received <= 32 * storage.MIB + 65536


def test_complete_pagination_and_drift_duplicate_missing_page():
    provider = Provider()
    for i in range(99):
        provider.items.append({"id": 1000 + i, "name": "unrelated-report"})
    with provider.reader() as reader:
        assert recover(reader).checkpoint is not None
        assert sum(r.url.params.get("page") == "2" for r in provider.requests) == 2
    def change(p):
        if p.list_calls == 3:
            p.items[0]["digest"] = "sha256:" + "d" * 64
    provider.list_calls = 0
    provider.list_hook = change
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None
    provider = Provider()
    provider.items.append(deepcopy(provider.items[0]))
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None
    provider = Provider()
    original = provider.handle
    def missing(request):
        if request.url.path.endswith("/artifacts"):
            return response({"total_count": 101, "artifacts": provider.items})
        return original(request)
    with github.GitHubStateReader(SCOPE, frozenset({SOURCE}), T, transport=httpx.MockTransport(missing)) as reader:
        assert recover(reader).checkpoint is None


def test_repository_identity_and_retention_backlog_are_bounded():
    provider = Provider()
    provider.repo["default_branch"] = "other"
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None and len(provider.requests) == 1
    provider = Provider()
    def changed(p):
        if p.repo_calls == 2:
            p.repo["default_branch"] = "other"
    provider.repo_hook = changed
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None
    provider = Provider(count=4)
    with provider.reader() as reader:
        assert recover(reader).checkpoint is None
        assert not any(r.url.path.endswith("/zip") for r in provider.requests)


@pytest.mark.parametrize("name", ["../state-bundle.json", "/state-bundle.json", "C:/state-bundle.json",
                                  "\\\\host\\share\\state-bundle.json", "CON", "sub/state-bundle.json"])
def test_zip_paths_are_never_extracted(name):
    body = b"{}"
    archive = zip_body(body, name=name)
    with pytest.raises(ValueError, match="unsafe"):
        github.state_zip(archive, storage.digest(archive), storage.digest(body))


@pytest.mark.parametrize("case", ["symlink", "directory", "duplicate", "method", "zip64", "ratio", "bad_zip", "bad_hash", "encrypted"])
def test_zip_rejects_links_bombs_and_invalid_formats(case):
    body = b"{}"
    kwargs = {}
    if case == "symlink":
        kwargs["mode"] = stat.S_IFLNK | 0o777
    elif case == "directory":
        kwargs["mode"] = stat.S_IFDIR | 0o755
    elif case == "duplicate":
        kwargs["duplicate"] = True
    elif case == "method":
        kwargs["method"] = zipfile.ZIP_BZIP2
    elif case == "zip64":
        kwargs["extra"] = b"\x01\x00\x00\x00"
    elif case == "ratio":
        body = b"x" * 1000000
    archive = zip_body(body, **kwargs)
    if case == "encrypted":
        encoded = bytearray(archive)
        encoded[6] |= 1  # Local header's encryption flag.
        encoded[encoded.index(b"PK\x01\x02") + 8] |= 1  # Central header.
        archive = bytes(encoded)
    if case == "bad_zip":
        archive = b"not-a-zip"
    with pytest.raises(ValueError):
        github.state_zip(archive, storage.digest(archive), "e" * 64 if case == "bad_hash" else storage.digest(body))


def test_shadow_adoption_cannot_override_local_truth_or_enable_notifications(tmp_path, monkeypatch):
    checkpoint = state.Checkpoint(generation=7, at=T, cursor=3, notifications_enabled=True)
    identity = (VERSION + "/A0", ("okx", "SPOT", "ALT-USDT"), "UP", T)
    ids = checkpoint.prepare([Transition(identity, "OBSERVED", T)], T)
    original = deepcopy(checkpoint.encode())
    store = state.StateStore(tmp_path / "state.json")
    with pytest.raises(Unavailable, match="writer_lock_required"):
        store.restore_shadow(checkpoint)
    with store.writer():
        restored = store.restore_shadow(checkpoint)
        assert restored.generation == 7 and not restored.notifications_enabled
        assert restored.ledger == checkpoint.ledger and restored.cursor == 3
        assert checkpoint.encode() == original
        with pytest.raises(Unavailable, match="fresh_workspace"):
            store.restore_shadow(checkpoint)
    assert store.load()[0].ledger[ids[0]]["status"] == "pending"
    bad = state.StateStore(tmp_path / "bad.json")
    bad.path.write_bytes(b"corrupt")
    with bad.writer(), pytest.raises(Unavailable, match="fresh_workspace"):
        bad.restore_shadow(checkpoint)
    assert bad.path.read_bytes() == b"corrupt"
    empty = state.StateStore(tmp_path / "empty.json")
    with empty.writer():
        monkeypatch.setattr(state.os, "replace", lambda *args: (_ for _ in ()).throw(OSError("replace failed")))
        with pytest.raises(OSError, match="replace failed"):
            empty.restore_shadow(checkpoint)
    assert not empty.path.exists() and not list(tmp_path.glob("*.tmp")) and not empty.lock_path.exists()


def test_shadow_adoption_seed_invalid_types_byte_limit_and_lock_token(tmp_path, monkeypatch):
    store = state.StateStore(tmp_path / "state.json")
    with store.writer():
        seed = store.restore_shadow(state.Checkpoint())
    assert seed.generation == 0 and store.load()[0].generation == 0
    other = state.StateStore(tmp_path / "other.json")
    with other.writer():
        with pytest.raises(ValueError):
            other.restore_shadow(state.Checkpoint(notifications_enabled="bad"))
        other.lock_path.write_text("different-token")
        with pytest.raises(Unavailable, match="writer_lock_required"):
            other.restore_shadow(seed)
    # The intentional wrong token is a fail-closed fixture, not a live writer.
    large = state.StateStore(tmp_path / "large.json")
    monkeypatch.setattr(state, "_digest", lambda v: "x" * (8 * storage.MIB))
    with large.writer(), pytest.raises(Unavailable, match="state_byte_limit"):
        large.restore_shadow(seed)
    assert not large.path.exists()


def test_early_provider_expiry_is_permitted_but_cannot_extend_state():
    provider = Provider()
    provider.items[-1]["expires_at"] = iso(T + 60 * MINUTE)
    with provider.reader() as reader:
        restored = recover(reader)
        assert restored.checkpoint is not None and restored.snapshot.expires_at == T + 60 * MINUTE
    raw = provider.raws[101]
    record = storage.state_candidate("state", raw, 1)
    with pytest.raises(ValueError, match="invalid_snapshot"):
        replace(record, expires_at=record.created_at + storage.DAY + 1)


def test_shadow_restore_rechecks_lock_after_encoding_and_dangling_target(tmp_path, monkeypatch):
    store = state.StateStore(tmp_path / "state.json")
    original = state._checkpoint_bytes
    def changed(checkpoint):
        body = original(checkpoint)
        store.lock_path.write_text("foreign-token")
        return body
    with store.writer():
        monkeypatch.setattr(state, "_checkpoint_bytes", changed)
        with pytest.raises(Unavailable, match="target_or_lock_changed"):
            store.restore_shadow(state.Checkpoint())
    assert not store.path.exists() and store.lock_path.read_text() == "foreign-token"
    monkeypatch.setattr(state, "_checkpoint_bytes", original)
    other = state.StateStore(tmp_path / "dangling.json")
    real_lexists = state.os.path.lexists
    with other.writer():
        monkeypatch.setattr(state.os.path, "lexists", lambda p: True if p == other.path else real_lexists(p))
        with pytest.raises(Unavailable, match="fresh_workspace"):
            other.restore_shadow(state.Checkpoint())
    assert not other.path.exists()


def test_original_save_envelope_is_byte_compatible_after_shared_encoder(tmp_path):
    store = state.StateStore(tmp_path / "state.json")
    checkpoint = state.Checkpoint()
    with store.writer():
        store.save(checkpoint, None)
    body = {"schema": "okx-checkpoint-v1", "rules_version": VERSION, "generation": 1,
            "at": 0, "cursor": 0, "notifications_enabled": False, "last_at": [], "active": [], "ledger": {}}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    expected = json.dumps({"state": body, "sha256": storage.digest(canonical)}, sort_keys=True,
                          separators=(",", ":"), allow_nan=False).encode()
    assert store.path.read_bytes() == expected
