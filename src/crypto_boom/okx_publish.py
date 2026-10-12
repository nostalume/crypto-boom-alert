"""Prepare durable intent, then publish once after a separately verified save.

There is no cache callback, auto-baseline, CLI or production caller. The Actions
consumer must verify the exact current-run cache on a fresh runner before passing
its checkpoint digest to publish_pending. A digest alone is not that evidence.
"""
import hashlib
import html
import json
import re
import time
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx

from .okx_state import StateStore
from .okx_storage import (
    DAY,
    MIB,
    RECEIPT_TAG,
    Scope,
    digest,
    extract_public_receipt,
    format_receipt_intent,
)
from .okx_trial import MINUTE, RULES, VERSION, Transition, Unavailable

START = "<!-- OKX_ALERT_START -->"
END = "<!-- OKX_ALERT_END -->"
SHA = re.compile(r"[0-9a-f]{40}")


def _utc(at):
    return datetime.fromtimestamp(at / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _display(value):
    if not isinstance(value, str) or not 0 < len(value) <= 128 or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError("publication_product_text")
    text = html.escape(value, quote=True)
    for literal, entity in (("|", "&#124;"), ("`", "&#96;"), ("\\", "&#92;"),
                            ("[", "&#91;"), ("]", "&#93;"), ("*", "&#42;"), ("_", "&#95;")):
        text = text.replace(literal, entity)
    return text


@dataclass(frozen=True)
class Publication:
    scope: Scope
    marker: str
    readme: bytes
    message: str


def render(checkpoint, ids, *, scope, source_sha, run_id, attempt, readme, observed_at):
    """Render a complete prepared batch, not prices, ranks or a trading promise."""
    if (not isinstance(readme, bytes) or len(readme) > MIB or type(observed_at) is not int
            or observed_at < checkpoint.at):
        raise ValueError("publication_inputs")
    marker = format_receipt_intent(checkpoint, ids, scope=scope, source_sha=source_sha,
                                   run_id=run_id, attempt=attempt)
    extract_public_receipt(readme)  # Reject ambiguous reserved markers before replacing a region.
    rows, stages = [], set()
    for key in sorted(ids):
        item = checkpoint.ledger[key]
        try:
            identity, stage = item["identity"]
            rule, product, direction, first = identity
            exchange, kind, symbol = product
        except (ValueError, TypeError) as exc:
            raise ValueError("publication_identity") from exc
        if (rule not in {VERSION + "/" + r for r in RULES} or exchange != "okx"
                or kind not in ("SPOT", "SWAP") or direction not in ("UP", "DOWN")
                or stage not in ("OBSERVED", "PERSISTENT") or type(first) is not int
                or first < 0 or first % MINUTE or first > item["transition_at"]
                or not 0 <= observed_at - item["transition_at"] <= DAY):
            raise ValueError("publication_identity_or_age")
        age = (observed_at - item["transition_at"]) // MINUTE
        timely = observed_at - item["transition_at"] <= 30 * MINUTE
        label = "及时 / TIMELY" if timely else "延迟回补 / BACKFILL"
        stages.add(stage)
        base = symbol.removesuffix('-SWAP').removesuffix('-USDT')
        rows.append(((not timely, stage != 'PERSISTENT', base, kind, key),
                     f"| {_display(base)} | {_display(symbol)} | {kind} | {direction} | {rule.rsplit('/', 1)[1]} | {stage} | "
                     f"{_utc(first)} | {_utc(item['transition_at'])} | {age}m · {label} |"))
    local = datetime.fromtimestamp(observed_at / 1000, timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    section = "\n".join([
        START, "## 最近 OKX 告警 / Latest alerts", "",
        f"源数据截止：{_utc(checkpoint.at)}。本批生成：{_utc(observed_at)} / 上海 {local}。",
        "仅记录价量异常；UP/DOWN 是观测方向，不是买卖建议。PERSISTENT 表示持续条件，不代表收益确认。",
        "本页不是实时扫描健康状态；无新告警不更新，扫描覆盖/失败请查看 Actions summary。", "",
        "展示顺序：及时项优先、同类持续项优先，再按币种分组；现货/永续保留独立事实，不是收益概率排名。", "",
        "| 币种分组 | 标的 | 市场 | 方向 | 规则 | 阶段 | 首次条件 | 本次条件 | 条件年龄 |",
        "|---|---|---|---|---|---|---|---|---|", *(row for _, row in sorted(rows)), "", marker, END,
    ])
    text = readme.decode("utf-8")
    if not text.count(START) and not text.count(END):
        if RECEIPT_TAG in text:
            raise ValueError("publication_orphan_receipt")
        text = text.rstrip() + "\n\n" + section + "\n"
    elif text.count(START) == text.count(END) == 1 and text.index(START) < text.index(END):
        before, after = text[:text.index(START)], text[text.index(END) + len(END):]
        if RECEIPT_TAG in before + after:
            raise ValueError("publication_orphan_receipt")
        text = before + section + after
    else:
        raise ValueError("publication_ambiguous_markers")
    result = text.encode("utf-8")
    if len(result) > MIB or extract_public_receipt(result) != marker:
        raise ValueError("publication_readme_limit_or_marker")
    stage_label = next(iter(stages)) if len(stages) == 1 else "MIXED"
    return Publication(scope, marker, result, f"OKX ALTCOIN ALERT {_utc(observed_at)} {stage_label}")


def _publication_state(store, observed_at, attempt):
    if not store.locked or not store._owns_lock():
        raise Unavailable("writer_lock_required")
    state, reason = store.load()
    if state is None:
        raise Unavailable(reason)
    if (type(attempt) is not int or attempt != 1 or type(observed_at) is not int
            or observed_at < state.at):
        raise ValueError("publication_run_or_time")
    return state


def prepare_batch(store: StateStore, transitions: list[Transition], *, scope: Scope, source_sha: str,
                  run_id: int, attempt: int, readme: bytes, observed_at: int):
    """Save pending under the caller's writer lock; quiet leaves state unchanged.

    This does not save remote cache or authorize a send. The returned view is for
    inspection; the later consumer rebuilds it from the verified saved checkpoint.
    """
    state = _publication_state(store, observed_at, attempt)
    if (not isinstance(transitions, list) or len(transitions) > 10000
            or any(not isinstance(t, Transition) for t in transitions)):
        raise ValueError("publication_transitions")
    # Never manufacture state from a market cache or auto-enable a shadow state.
    candidate = deepcopy(state)
    eligible = [t for t in transitions if 0 <= observed_at - t.at <= DAY]
    ids = candidate.prepare(eligible, state.at)
    if not ids:
        return None
    # Render the projected CAS generation once; StateStore owns its actual commit.
    publication = render(replace(candidate, generation=candidate.generation + 1), ids,
                         scope=scope, source_sha=source_sha, run_id=run_id,
                         attempt=attempt, readme=readme, observed_at=observed_at)
    store.save(candidate, state.generation)
    return publication


def publish_pending(store: StateStore, *, verified_checkpoint_sha256: str, scope: Scope,
                    source_sha: str, run_id: int, attempt: int, readme: bytes,
                    observed_at: int, publish):
    """Consume exactly the independently verified pending checkpoint once.

    Native Actions save/restore and provider admission happen outside this owner.
    No unknown retry, arbitrary batch selection or implicit baseline. A confirmed
    result is a Git commit observation, not email delivery; final remote save is
    still mandatory for the workflow to report successful completion.
    """
    state = _publication_state(store, observed_at, attempt)
    with store.path.open('rb') as stream:
        raw = stream.read(8 * MIB + 1)
    if (not isinstance(verified_checkpoint_sha256, str)
            or re.fullmatch(r'[0-9a-f]{64}', verified_checkpoint_sha256) is None
            or len(raw) > 8 * MIB or digest(raw) != verified_checkpoint_sha256):
        raise Unavailable("durable_intent_not_confirmed")
    if not state.notifications_enabled or any(v['status'] == 'unknown' for v in state.ledger.values()):
        raise Unavailable('publication_baseline_or_unknown')
    ids = tuple(sorted(k for k, v in state.ledger.items() if v['status'] == 'pending'))
    if not ids or len({state.ledger[k]['batch'] for k in ids}) != 1:
        raise Unavailable('one_pending_batch_required')
    publication = render(state, ids, scope=scope, source_sha=source_sha, run_id=run_id,
                         attempt=attempt, readme=readme, observed_at=observed_at)
    state.unknown(ids)
    store.save(state, state.generation)
    # Both known failure and unknown acceptance remain fail-closed. Do not catch
    # cancellation and do not try a second update against another branch head.
    commit = publish(publication)
    if not isinstance(commit, str) or SHA.fullmatch(commit) is None:
        raise Unavailable("publication_not_observed")
    state.reconcile(ids, state.ledger[ids[0]]["batch"])
    store.save(state, state.generation)
    return commit


class GitHubPublisher:
    """One branch update anchored to an explicit head; no CLI/environment token.

    HTTPX transport injection is the only network substitution. This owns no
    baseline, carrier admission or cross-run recovery. force=false avoids normal
    concurrent overwrite; it is not a distributed lock or arbitrary force-reset
    protection. A failed call may leave unreferenced tree/commit objects.
    """
    def __init__(self, scope: Scope, repository_id: int, expected_head: str, readme: bytes, *,
                 token: str, transport=None, clock=time.monotonic):
        branch = scope.ref.removeprefix("refs/heads/") if isinstance(scope, Scope) else ""
        if (not isinstance(scope, Scope) or type(repository_id) is not int or repository_id < 1
                or not isinstance(expected_head, str) or SHA.fullmatch(expected_head) is None
                or not isinstance(readme, bytes) or len(readme) > MIB
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,200}", branch) is None
                or ".." in branch or "//" in branch or branch.endswith(("/", ".", ".lock"))
                or not isinstance(token, str) or not token or any(not 33 <= ord(c) <= 126 for c in token)):
            raise ValueError("publisher_authority")
        self.scope, self.repository_id, self.head, self.readme = scope, repository_id, expected_head, readme
        self.token, self.transport, self.clock = token, transport, clock
        self.used = False

    def __call__(self, publication: Publication):
        if self.used:
            raise Unavailable("publisher_already_used")
        self.used = True
        if (not isinstance(publication, Publication) or publication.scope != self.scope
                or not isinstance(publication.readme, bytes) or len(publication.readme) > MIB
                or extract_public_receipt(publication.readme) != publication.marker
                or not isinstance(publication.message, str) or len(publication.message) > 200
                or not publication.message.startswith("OKX ALTCOIN ALERT ")
                or "\n" in publication.message or "\r" in publication.message):
            raise ValueError("publisher_batch")
        self.deadline, self.requests, self.received = self.clock() + 60, 0, 0
        headers = {"Accept": "application/vnd.github+json", "Accept-Encoding": "identity",
                   "Authorization": "Bearer " + self.token, "X-GitHub-Api-Version": "2026-03-10"}
        with httpx.Client(headers=headers, transport=self.transport, trust_env=False,
                          follow_redirects=False, timeout=httpx.Timeout(15, connect=5)) as client:
            self.client = client
            return self._publish(publication)

    def _json(self, method, path, body=None):
        if self.clock() >= self.deadline or self.requests >= 12:
            raise Unavailable("publisher_request_or_deadline_limit")
        self.requests += 1
        self.client.cookies.clear()
        try:
            with self.client.stream(method, "https://api.github.com/repos/" + self.scope.repository + path,
                                    json=body) as response:
                if response.status_code != (201 if method == "POST" else 200):
                    raise Unavailable("publisher_http_" + str(response.status_code))
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise Unavailable("publisher_encoded_response")
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isdecimal() or int(length) > 2 * MIB):
                    raise Unavailable("publisher_declared_byte_limit")
                raw = bytearray()
                for part in response.iter_raw(chunk_size=65536):
                    self.received += len(part)
                    if self.clock() >= self.deadline or self.received > 8 * MIB or len(raw) + len(part) > 2 * MIB:
                        raise Unavailable("publisher_response_budget")
                    raw.extend(part)
                if length is not None and len(raw) != int(length):
                    raise Unavailable("publisher_response_length")
                if self.clock() >= self.deadline:
                    raise Unavailable("publisher_response_budget")
                result = json.loads(raw, object_pairs_hook=_pairs)
                if not isinstance(result, dict):
                    raise ValueError("publisher_response_object")
                return result
        except httpx.HTTPError:
            raise Unavailable("publisher_transport_failure") from None

    def _ref(self, body, expected):
        obj = body.get("object")
        if (body.get("ref") != self.scope.ref or not isinstance(obj, dict)
                or obj.get("type") != "commit" or obj.get("sha") != expected):
            raise ValueError("publisher_ref_mismatch")

    def _publish(self, publication):
        repo = self._json("GET", "")
        if (repo.get("id") != self.repository_id or type(repo.get("id")) is not int
                or repo.get("full_name") != self.scope.repository or repo.get("private") is not False):
            raise ValueError("publisher_repository_mismatch")
        ref_path = "/git/ref/" + quote(self.scope.ref.removeprefix("refs/"), safe="/")
        self._ref(self._json("GET", ref_path), self.head)
        parent = self._json("GET", "/git/commits/" + self.head)
        parent_tree = parent.get("tree")
        tree_sha = parent_tree.get("sha") if isinstance(parent_tree, dict) else None
        if parent.get("sha") != self.head or not isinstance(tree_sha, str) or SHA.fullmatch(tree_sha) is None:
            raise ValueError("publisher_parent_mismatch")
        tree = self._json("GET", "/git/trees/" + tree_sha)
        rows = tree.get("tree")
        if tree.get("sha") != tree_sha or tree.get("truncated") is not False or not isinstance(rows, list):
            raise ValueError("publisher_tree_incomplete")
        entries = [r for r in rows if isinstance(r, dict) and r.get("path") == "README.md"]
        expected_blob = _git_blob(self.readme)
        if (len(entries) != 1 or entries[0].get("type") != "blob" or entries[0].get("mode") != "100644"
                or entries[0].get("sha") != expected_blob):
            raise ValueError("publisher_readme_mismatch")
        # Comparing the git blob hash binds the explicit checkout README bytes.
        updated_blob = _git_blob(publication.readme)
        new_tree = self._json("POST", "/git/trees", {"base_tree": tree_sha, "tree": [{"path": "README.md",
                             "mode": "100644", "type": "blob", "content": publication.readme.decode("utf-8")}]})
        new_tree_sha = new_tree.get("sha")
        new_rows = new_tree.get("tree")
        changed = [r for r in new_rows if isinstance(r, dict) and r.get("path") == "README.md"] if isinstance(new_rows, list) else []
        if (not isinstance(new_tree_sha, str) or SHA.fullmatch(new_tree_sha) is None
                or new_tree.get("truncated") is not False or len(changed) != 1
                or changed[0].get("sha") != updated_blob or changed[0].get("mode") != "100644"
                or changed[0].get("type") != "blob"):
            raise ValueError("publisher_created_tree_mismatch")
        committed = self._json("POST", "/git/commits", {"message": publication.message,
                               "tree": new_tree_sha, "parents": [self.head]})
        sha = committed.get("sha")
        if not isinstance(sha, str) or SHA.fullmatch(sha) is None:
            raise ValueError("publisher_created_commit_mismatch")
        self._commit(committed, sha, new_tree_sha, publication.message)
        self._ref(self._json("PATCH", ref_path.replace("/git/ref/", "/git/refs/"),
                            {"sha": sha, "force": False}), sha)
        self._ref(self._json("GET", ref_path), sha)
        self._commit(self._json("GET", "/git/commits/" + sha), sha, new_tree_sha, publication.message)
        return sha

    def _commit(self, body, sha, tree_sha, message):
        parents = body.get("parents")
        tree = body.get("tree")
        if (body.get("sha") != sha or not isinstance(tree, dict) or tree.get("sha") != tree_sha
                or body.get("message") != message
                or not isinstance(parents, list) or len(parents) != 1
                or not isinstance(parents[0], dict) or parents[0].get("sha") != self.head):
            raise ValueError("publisher_commit_mismatch")


def _git_blob(raw):
    return hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("publisher_duplicate_key")
        result[key] = value
    return result
