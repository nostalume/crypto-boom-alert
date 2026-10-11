"""Offline publication lifecycle and provider contracts, never real email/cache."""
import json
from dataclasses import replace
from json import dumps

import httpx
import pytest

from crypto_boom import okx_publish as publication
from crypto_boom.okx_state import Checkpoint, StateStore
from crypto_boom.okx_storage import DAY, MIB, Scope, digest, parse_public_receipt
from crypto_boom.okx_trial import MINUTE, VERSION, Transition, Unavailable

T = 1780000020000
SCOPE = Scope("nostalume/crypto-boom-alert", 456, "refs/heads/codex/notification-test")
SOURCE, HEAD, TREE, NEW_TREE, COMMIT = (letter * 40 for letter in "abcde")
README = b"# Existing project\n\n<!-- HOURLY_ALERT_RECEIPT_V1:legacy -->\n"


def response(status, *, json=None, content=None, headers=None):
    raw = dumps(json).encode() if content is None else content
    return httpx.Response(status, headers=headers, stream=httpx.ByteStream(raw))


def change(symbol="ALT-USDT", stage="OBSERVED", at=T, first=None, direction="UP", kind="SPOT"):
    return Transition((VERSION + "/A0", ("okx", kind, symbol), direction, at if first is None else first), stage, at)


def seeded(tmp_path, enabled=True):
    store = StateStore(tmp_path / "checkpoint.json")
    with store.writer():
        state = Checkpoint(at=T)
        store.save(state, None)
        state.notifications_enabled = enabled  # Explicit test-only human baseline.
        store.save(state, state.generation)
    return store


def rendered(changes=None, readme=README, observed_at=T):
    state = Checkpoint(generation=3, at=T, notifications_enabled=True)
    ids = state.prepare(changes or [change()], T)
    return publication.render(state, ids, scope=SCOPE, source_sha=SOURCE, run_id=7, attempt=1,
                              readme=readme, observed_at=observed_at)


def send(store, changes=None, saver=None, publisher=None, **kwargs):
    options = dict(scope=SCOPE, source_sha=SOURCE, run_id=7, attempt=1, readme=README, observed_at=T)
    options.update(kwargs)
    prepared = publication.prepare_batch(store, [change()] if changes is None else changes, **options)
    if prepared is None:
        return None
    # Fake carrier boundary exists only in tests, not as a production callback.
    verified = (saver or digest)(store.path.read_bytes())
    return publication.publish_pending(store, verified_checkpoint_sha256=verified,
                                       publish=publisher or (lambda request: COMMIT), **options)


class Provider:
    def __init__(self, fault=None):
        self.fault, self.requests, self.head = fault, [], HEAD
        self.message, self.updated_blob, self.patch_calls = None, None, 0

    def handle(self, request):
        path = request.url.path.removeprefix("/repos/" + SCOPE.repository)
        method = request.method
        self.requests.append((method, path, dict(request.headers)))
        assert request.url.host == "api.github.com"
        assert request.headers["Authorization"] == "Bearer test-only-secret"
        assert request.headers["X-GitHub-Api-Version"] == "2026-03-10"
        assert "cookie" not in request.headers
        body = json.loads(request.content) if request.content else None
        if self.fault == "redirect":
            return response(302, headers={"Location": "https://evil.example/secret"})
        if self.fault == "oversized":
            return response(200, headers={"Content-Length": str(2 * MIB + 1)}, content=b"{}")
        if self.fault == "encoding":
            return response(200, headers={"Content-Encoding": "br"}, content=b"{}")
        if self.fault == "duplicate":
            return response(200, content=b'{"id":1,"id":2}')
        if self.fault == "root_list":
            return response(200, json=[])
        if self.fault == "transport_secret":
            raise httpx.ReadTimeout("test-only-secret https://evil.example/secret", request=request)
        if method == "GET" and not path:
            result = {"id": 1391009742, "full_name": SCOPE.repository, "private": False}
            if self.fault == "foreign_repo":
                result["id"] += 1
            if self.fault == "private":
                result["private"] = True
            return response(200, json=result)
        if method == "GET" and path == "/git/ref/" + SCOPE.ref.removeprefix("refs/"):
            ref = {"ref": SCOPE.ref, "object": {"type": "commit", "sha": self.head}}
            if self.fault == "head":
                ref["object"]["sha"] = "f" * 40
            if self.fault == "post_ref" and self.patch_calls:
                ref["object"]["sha"] = "f" * 40
            return response(200, json=ref)
        if method == "GET" and path == "/git/commits/" + HEAD:
            return response(200, json={"sha": HEAD, "tree": {"sha": TREE}})
        if method == "GET" and path == "/git/trees/" + TREE:
            rows = [{"path": "README.md", "type": "blob", "mode": "100644", "sha": publication._git_blob(README)},
                    {"path": "src", "type": "tree", "mode": "040000", "sha": "f" * 40}]
            if self.fault == "readme":
                rows[0]["sha"] = "f" * 40
            if self.fault == "symlink":
                rows[0]["mode"] = "120000"
            if self.fault == "duplicate_readme":
                rows.append(rows[0])
            return response(200, json={"sha": TREE, "truncated": self.fault == "truncated", "tree": rows})
        if method == "POST" and path == "/git/trees":
            assert body["base_tree"] == TREE
            assert len(body["tree"]) == 1
            row = body["tree"][0]
            assert {k: row[k] for k in ("path", "mode", "type")} == {
                "path": "README.md", "mode": "100644", "type": "blob"}
            self.updated_blob = publication._git_blob(row["content"].encode())
            return response(201, json={"sha": NEW_TREE, "truncated": False, "tree": [{
                "path": "README.md", "mode": "100644", "type": "blob",
                "sha": "f" * 40 if self.fault == "created_tree" else self.updated_blob}]})
        if method == "POST" and path == "/git/commits":
            assert body["tree"] == NEW_TREE and body["parents"] == [HEAD]
            self.message = body["message"]
            return response(201, json=self.commit(bad=self.fault == "created_commit"))
        if method == "PATCH" and path == "/git/refs/" + SCOPE.ref.removeprefix("refs/"):
            self.patch_calls += 1
            assert body == {"sha": COMMIT, "force": False}
            if self.fault == "race":
                self.head = "f" * 40
                return response(422, json={"message": "test-only-secret"})
            self.head = COMMIT
            if self.fault == "accepted_timeout":
                raise httpx.ReadTimeout("test-only-secret", request=request)
            return response(200, json={"ref": SCOPE.ref, "object": {"type": "commit", "sha": COMMIT}})
        if method == "GET" and path == "/git/commits/" + COMMIT:
            return response(200, json=self.commit(bad=self.fault == "post_commit"))
        raise AssertionError((method, path))

    def commit(self, bad=False):
        return {"sha": COMMIT, "tree": {"sha": NEW_TREE}, "message": self.message,
                "parents": [{"sha": "f" * 40 if bad else HEAD}]}

    def publisher(self, **kwargs):
        return publication.GitHubPublisher(SCOPE, 1391009742, HEAD, README, token="test-only-secret",
                                           transport=httpx.MockTransport(self.handle), **kwargs)


def test_complete_batch_saved_before_publishing_then_confirmed_and_duplicate_no_effects(tmp_path):
    store, calls, carrier = seeded(tmp_path), [], []
    changes = [change(), change("ALT-USDT-SWAP", "PERSISTENT", first=T - 10 * MINUTE, kind="SWAP", direction="DOWN")]

    def save(raw):
        calls.append("save")
        state = json.loads(raw)["state"]
        assert {r["status"] for r in state["ledger"].values()} == {"pending"}
        carrier.append(raw)
        return digest(raw)

    def publish(request):
        calls.append("publish")
        state, _ = store.load()
        assert {r["status"] for r in state.ledger.values()} == {"unknown"}
        receipt = parse_public_receipt(request.marker, scope=SCOPE, source_sha=SOURCE, run_id=7, attempt=1, published_at=T)
        assert receipt.generation == json.loads(carrier[0])["state"]["generation"]
        assert len(receipt.ids) == 2
        assert "MIXED" in request.message and "CONFIRMED" not in request.message
        assert "DOWN" in request.readme.decode() and "UP" in request.readme.decode()
        return COMMIT

    with store.writer():
        assert send(store, changes, save, publish) == COMMIT
        after = store.path.read_bytes()
        assert send(store, changes, save, publish) is None
        assert store.path.read_bytes() == after
    assert calls == ["save", "publish"]
    assert json.loads(carrier[0])["state"]["generation"] == 3
    state, _ = store.load()
    assert state.generation == 5
    assert {r["status"] for r in state.ledger.values()} == {"confirmed"}


@pytest.mark.parametrize("changes", [[], [change(stage="ENDED")], [change(at=T - DAY - MINUTE)], [change(at=T + MINUTE)]])
def test_no_new_message_no_state_or_network_effect(tmp_path, changes):
    store = seeded(tmp_path)
    before = store.path.read_bytes()
    def forbidden(*args):
        pytest.fail("no-signal effect")
    with store.writer():
        assert send(store, changes, forbidden, forbidden) is None
    assert store.path.read_bytes() == before


@pytest.mark.parametrize("kind", ["disabled", "missing", "corrupt", "pending", "unknown", "unlocked"])
def test_baseline_lock_and_unresolved_states_never_send(tmp_path, kind):
    store = seeded(tmp_path, kind != "disabled")
    if kind == "missing":
        store.path.unlink()
    elif kind == "corrupt":
        store.path.write_bytes(b"broken")
    elif kind in ("pending", "unknown"):
        with store.writer():
            state, _ = store.load()
            ids = state.prepare([change()], T)
            if kind == "unknown":
                state.unknown(ids)
            store.save(state, state.generation)
    def forbidden(*args):
        pytest.fail("blocked effect")
    if kind == "unlocked":
        with pytest.raises(Unavailable):
            send(store, saver=forbidden, publisher=forbidden)
    else:
        with store.writer(), pytest.raises(Unavailable):
            send(store, saver=forbidden, publisher=forbidden)


@pytest.mark.parametrize("failure", ["save_rejected", "save_error", "save_cancel", "publish_error", "publish_cancel", "publish_bad_result"])
def test_unknown_results_never_retry_and_remain_durable(tmp_path, failure):
    store, calls = seeded(tmp_path), []
    def save(raw):
        calls.append("save")
        if failure == "save_rejected":
            return "wrong"
        if failure == "save_error":
            raise OSError("carrier failure")
        if failure == "save_cancel":
            raise KeyboardInterrupt()
        return digest(raw)
    def publish(request):
        calls.append("publish")
        if failure == "publish_error":
            raise OSError("publication failure")
        if failure == "publish_cancel":
            raise KeyboardInterrupt()
        return {"accepted": True}
    with store.writer():
        with pytest.raises((Unavailable, OSError, KeyboardInterrupt)):
            send(store, saver=save, publisher=publish)
        state, _ = store.load()
        assert {r["status"] for r in state.ledger.values()} == ({"pending"} if failure.startswith("save") else {"unknown"})
        before = list(calls)
        with pytest.raises(Unavailable, match="unresolved"):
            send(store, saver=save, publisher=publish)
        assert calls == before


@pytest.mark.parametrize("phase", [1, 2, 3])
def test_local_cas_failure_does_not_continue_or_fake_confirm(tmp_path, monkeypatch, phase):
    store, calls, save_count = seeded(tmp_path), [], 0
    real_save = store.save
    def failed_save(*args):
        nonlocal save_count
        save_count += 1
        if save_count == phase:
            raise OSError("atomic save failed")
        return real_save(*args)
    monkeypatch.setattr(store, "save", failed_save)
    def save(raw):
        calls.append("save")
        return digest(raw)
    def publish(request):
        calls.append("publish")
        return COMMIT
    with store.writer(), pytest.raises(OSError):
        send(store, saver=save, publisher=publish)
    state, _ = store.load()
    assert calls == {1: [], 2: ["save"], 3: ["save", "publish"]}[phase]
    assert {r["status"] for r in state.ledger.values()} == {1: set(), 2: {"pending"}, 3: {"unknown"}}[phase]


def test_render_labels_boundaries_unicode_and_only_latest_region():
    changes = [change("币|种`-USDT"), change("OLD-USDT", at=T - 31 * MINUTE),
               change("EDGE-USDT", at=T - 30 * MINUTE), change("DAY-USDT", at=T - DAY)]
    request = rendered(changes)
    text = request.readme.decode()
    assert "币&#124;种&#96;-USDT" in text
    assert "30m · 及时" in text and "31m · 延迟回补" in text and "1440m · 延迟回补" in text
    assert "最近 OKX 告警" in text and "不是实时扫描" in text and "上海" in text
    assert text.startswith(README.decode())
    new = rendered([change("NEXT-USDT")], readme=request.readme)
    assert "NEXT-USDT" in new.readme.decode() and "OLD-USDT" not in new.readme.decode()
    assert new.readme.decode().count(publication.START) == 1


@pytest.mark.parametrize("readme", [b"<!-- OKX_ALERT_START -->", b"<!-- OKX_ALERT_END -->", b"<!-- OKX_ALERT_END --><!-- OKX_ALERT_START -->",
                                    b"OKX_ALERT_RECEIPT_V1:orphan", b"\xff", b"x" * (MIB + 1)],
                         ids=["start_only", "end_only", "reversed", "orphan", "utf8", "oversized"])
def test_bad_readme_has_no_mutation(tmp_path, readme):
    store = seeded(tmp_path)
    before = store.path.read_bytes()
    with store.writer(), pytest.raises((ValueError, UnicodeError)):
        publication.prepare_batch(store, [change()], scope=SCOPE, source_sha=SOURCE, run_id=7, attempt=1,
                                  readme=readme, observed_at=T)
    assert store.path.read_bytes() == before


@pytest.mark.parametrize("transition", [change("BAD\n-USDT"), change(direction="BUY"), change(first=T + MINUTE),
                                        Transition(("binance/v1", ("binance", "SPOT", "ALTUSDT"), "UP", T), "OBSERVED", T)])
def test_invalid_identity_is_not_published_or_saved(tmp_path, transition):
    store = seeded(tmp_path)
    before = store.path.read_bytes()
    with store.writer(), pytest.raises(ValueError):
        send(store, [transition])
    assert store.path.read_bytes() == before


def test_mock_git_update_is_anchored_single_attempt_and_read_back():
    provider = Provider()
    sender = provider.publisher()
    assert sender(rendered()) == COMMIT
    assert len(provider.requests) == 9 and provider.patch_calls == 1
    assert provider.head == COMMIT
    assert [(m, p) for m, p, _ in provider.requests][-2:] == [
        ("GET", "/git/ref/heads/codex/notification-test"), ("GET", "/git/commits/" + COMMIT)]
    with pytest.raises(Unavailable, match="already_used"):
        sender(rendered())
    assert provider.patch_calls == 1


@pytest.mark.parametrize("fault", ["foreign_repo", "private", "head", "readme", "symlink", "duplicate_readme", "truncated",
                                  "created_tree", "created_commit", "race", "accepted_timeout", "post_ref", "post_commit",
                                  "redirect", "oversized", "encoding", "duplicate", "root_list", "transport_secret"])
def test_provider_fault_blocks_confirmation_and_no_retry(tmp_path, fault):
    store, provider = seeded(tmp_path), Provider(fault)
    publisher = provider.publisher()
    with store.writer():
        with pytest.raises((Unavailable, ValueError)) as caught:
            send(store, publisher=publisher)
        state, _ = store.load()
        assert {r["status"] for r in state.ledger.values()} == {"unknown"}
        requests = list(provider.requests)
        with pytest.raises(Unavailable, match="unresolved"):
            send(store, publisher=publisher)
        assert provider.requests == requests
    assert provider.patch_calls <= 1
    assert "test-only-secret" not in str(caught.value) and "evil.example" not in str(caught.value)
    if fault == "accepted_timeout":
        assert provider.head == COMMIT  # Remote effect can exist while local result is unknown.


def test_scope_mismatch_before_http_and_explicit_authority_validation():
    provider = Provider()
    with pytest.raises(ValueError, match="batch"):
        provider.publisher()(replace(rendered(), scope=Scope(SCOPE.repository, 999, SCOPE.ref)))
    assert not provider.requests
    for kwargs in ({"token": "bad\nsecret"}, {"expected_head": "main"}, {"repository_id": True},
                   {"readme": b"x" * (MIB + 1)}, {"scope": Scope(SCOPE.repository, 456, "refs/heads/a/../main")}):
        options = {"scope": SCOPE, "repository_id": 1391009742, "expected_head": HEAD, "readme": README, "token": "test"}
        options.update(kwargs)
        with pytest.raises(ValueError):
            publication.GitHubPublisher(**options)


def test_deadline_blocks_before_network():
    times = iter([0, 61])
    provider = Provider()
    with pytest.raises(Unavailable, match="deadline"):
        provider.publisher(clock=lambda: next(times))(rendered())
    assert not provider.requests


@pytest.mark.parametrize("kwargs", [{"attempt": 2}, {"attempt": True}, {"run_id": True}, {"source_sha": "main"},
                                   {"observed_at": T - 1}, {"observed_at": True}, {"scope": None}])
def test_producer_and_time_errors_have_no_side_effects(tmp_path, kwargs):
    store = seeded(tmp_path)
    before = store.path.read_bytes()
    options = dict(scope=SCOPE, source_sha=SOURCE, run_id=7, attempt=1, readme=README, observed_at=T)
    options.update(kwargs)
    with store.writer(), pytest.raises(ValueError):
        publication.prepare_batch(store, [change()], **options)
    assert store.path.read_bytes() == before


def test_saver_changing_generation_does_not_allow_publish(tmp_path):
    store = seeded(tmp_path)
    def save(raw):
        current, _ = store.load()
        store.save(current, current.generation)
        return digest(raw)
    with store.writer(), pytest.raises(Unavailable, match="durable_intent"):
        send(store, saver=save, publisher=lambda x: pytest.fail("publish after stale generation"))
    state, _ = store.load()
    assert {r["status"] for r in state.ledger.values()} == {"pending"}


def test_age_label_uses_observed_time_not_just_source_cutoff(tmp_path):
    request = rendered([change(at=T - 30 * MINUTE)], observed_at=T + 1)
    assert "30m · 延迟回补" in request.readme.decode()
    store = seeded(tmp_path)
    before = store.path.read_bytes()
    with store.writer():
        result = publication.prepare_batch(store, [change(at=T - DAY)], scope=SCOPE, source_sha=SOURCE,
                                           run_id=7, attempt=1, readme=README, observed_at=T + 1)
    assert result is None and store.path.read_bytes() == before


def test_ambiguous_existing_receipt_not_silently_overwritten():
    request = rendered()
    old = request.readme.replace(publication.END.encode(), (request.marker + "\n" + publication.END).encode())
    with pytest.raises(ValueError, match="duplicate"):
        rendered(readme=old)


@pytest.mark.parametrize("mode", ["length", "single_bytes", "total_bytes", "during_stream", "request_count"])
def test_transport_resource_guards(mode, monkeypatch):
    provider = Provider()
    publisher = provider.publisher()
    real = provider.handle
    def handler(request):
        answer = real(request)
        if mode == "length":
            return response(200, content=b"{}", headers={"Content-Length": "1"})
        if mode == "single_bytes":
            return response(200, content=b" " * (2 * MIB + 1))
        if mode == "total_bytes":
            raw = answer.read()
            value = json.loads(raw)
            value["padding"] = "x" * MIB
            return response(answer.status_code, json=value)
        if mode == "during_stream":
            publisher.clock = lambda: 61
        if mode == "request_count":
            publisher.requests = 12
        return answer
    publisher.transport = httpx.MockTransport(handler)
    if mode == "during_stream":
        publisher.clock = lambda: 0
    with pytest.raises(Unavailable):
        publisher(rendered())
    assert len(provider.requests) <= 9 and provider.patch_calls <= 1


def test_markdown_symbols_cannot_turn_product_into_link_or_emphasis():
    text = rendered([change("[币_种*](example)-USDT")]).readme.decode()
    assert "&#91;币&#95;种&#42;&#93;(example)-USDT" in text
    assert "[币_种*](example)" not in text


def test_provider_success_is_observed_commit_not_email_delivery(tmp_path):
    store, provider = seeded(tmp_path), Provider()
    with store.writer():
        result = send(store, publisher=provider.publisher())
    assert result == COMMIT
    state, _ = store.load()
    assert {r["status"] for r in state.ledger.values()} == {"confirmed"}
    # The carrier contains only durable pending. Final remote confirmed save and
    # email receipt require the separately authorized workflow/user observation.


def test_restored_shadow_cannot_implicitly_send(tmp_path):
    source = seeded(tmp_path / "source")
    state, _ = source.load()
    store = StateStore(tmp_path / "restored" / "checkpoint.json")
    with store.writer():
        store.restore_shadow(state)
        with pytest.raises(Unavailable, match="baseline"):
            send(store, publisher=lambda x: pytest.fail("shadow publication"))
