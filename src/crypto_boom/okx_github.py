"""Bounded, read-only GitHub state artifacts. Not a publisher or quota authority.

No CLI or production workflow calls this reader. Tests inject MockTransport;
actual authenticated download/producer/frontier acceptance remains separate.
"""
import io
import json
import re
import stat
import time
import zipfile
import zlib
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime
from urllib.parse import urlsplit

import httpx

from crypto_boom.okx_storage import (
    BUNDLE_LIMIT,
    DAY,
    MIB,
    Inventory,
    Scope,
    decode_state_bundle,
    digest,
)

PREFIX = "okx-state-v1-"
NAME = re.compile(r"okx-state-v1-g(0|[1-9][0-9]{0,18})-a([1-9][0-9]{0,8})-([0-9a-f]{64})")
SHA = re.compile(r"[0-9a-f]{40}")


def _int(value, minimum=0):
    return type(value) is int and value >= minimum


def _timestamp(value):
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z", value) is None:
        raise ValueError("provider_timestamp")
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def _pairs(pairs):
    body = {}
    for key, value in pairs:
        if key in body:
            raise ValueError("provider_duplicate_key")
        body[key] = value
    return body


def state_zip(raw: bytes, archive_sha: str, bundle_sha: str) -> bytes:
    """Validate both hashes and decompress one bounded regular file in memory."""
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 10 * MIB or digest(raw) != archive_sha:
        raise ValueError("archive_digest_or_size")
    try:
        return _inflate_state_zip(raw, bundle_sha)
    except (zipfile.BadZipFile, RuntimeError, EOFError, NotImplementedError, zlib.error) as exc:
        raise ValueError("archive_invalid") from exc


def _inflate_state_zip(raw: bytes, bundle_sha: str) -> bytes:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        if len(entries) != 1:
            raise ValueError("archive_entry_count")
        entry = entries[0]
        mode = entry.external_attr >> 16
        extra = entry.extra
        while extra:
            if len(extra) < 4:
                raise ValueError("archive_extra_field")
            key, length = int.from_bytes(extra[:2], "little"), int.from_bytes(extra[2:4], "little")
            if key == 1 or len(extra) < 4 + length:
                raise ValueError("archive_zip64_or_extra")
            extra = extra[4 + length:]
        if (entry.filename != "state-bundle.json" or entry.orig_filename != entry.filename or entry.is_dir()
                or stat.S_IFMT(mode) not in (0, stat.S_IFREG) or entry.flag_bits & 1
                or entry.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
                or entry.extract_version >= 45 or not 0 < entry.file_size <= BUNDLE_LIMIT
                or entry.compress_size > 10 * MIB or entry.file_size > max(entry.compress_size, 1) * 200):
            raise ValueError("archive_unsafe_entry")
        result = bytearray()
        with archive.open(entry) as stream:
            while True:
                part = stream.read(min(65536, BUNDLE_LIMIT + 1 - len(result)))
                if not part:
                    break
                result.extend(part)
                if len(result) > BUNDLE_LIMIT:
                    raise ValueError("archive_expansion_limit")
        body = bytes(result)
        if len(body) != entry.file_size or digest(body) != bundle_sha:
            raise ValueError("bundle_digest_or_size")
        return body


class GitHubStateReader:
    """Complete bounded inventory of this scope, retaining at most three objects.

    All owned objects must be valid; a damaged leftover is diagnostic, not silently
    hidden. A fresh inventory is compared after downloads to detect drift. This is
    an observational fence, NOT distributed CAS or proof that deleted history never
    existed. Callers must supply trusted minimum_generation/receipt evidence.
    """
    def __init__(self, scope: Scope, allowed_sources: frozenset[str], now: int, *,
                 token: str | None = None, transport=None, clock=time.monotonic):
        if (not isinstance(scope, Scope) or any(p in (".", "..") for p in scope.repository.split("/"))
                or not isinstance(allowed_sources, frozenset) or not 0 < len(allowed_sources) <= 100
                or any(not isinstance(s, str) or SHA.fullmatch(s) is None for s in allowed_sources)
                or not _int(now) or (token is not None and (not isinstance(token, str) or not token
                    or any(ord(c) < 33 or ord(c) > 126 for c in token)))):
            raise ValueError("reader_authority")
        self.scope, self.allowed_sources, self.now = scope, allowed_sources, now
        self.token, self.transport, self.clock = token, transport, clock
        self.active = False
        self.requests = self.received = 0
        self.bodies = {}

    def __enter__(self):
        if self.active:
            raise ValueError("reader_already_active")
        self.stack = ExitStack()
        try:
            headers = {"Accept": "application/vnd.github+json", "Accept-Encoding": "identity",
                       "X-GitHub-Api-Version": "2022-11-28"}
            if self.token is not None:
                headers["Authorization"] = "Bearer " + self.token
            options = dict(transport=self.transport, trust_env=False, follow_redirects=False,
                           timeout=httpx.Timeout(20, connect=5))
            self.api = self.stack.enter_context(httpx.Client(headers=headers, **options))
            self.blob = self.stack.enter_context(httpx.Client(headers={"Accept-Encoding": "identity"}, **options))
            self.deadline = self.clock() + 120
            self.active = True
            return self
        except BaseException:
            self.stack.close()
            raise

    def __exit__(self, *args):
        self.active = False
        self.bodies.clear()
        return self.stack.__exit__(*args)

    def _check(self):
        if not self.active or self.clock() >= self.deadline:
            raise OSError("reader_closed_or_deadline")

    def _request(self, url, *, blob=False, limit=2 * MIB, redirect=False):
        self._check()
        if self.requests >= 32:
            raise OSError("reader_request_limit")
        self.requests += 1
        client = self.blob if blob else self.api
        client.cookies.clear()  # Even Set-Cookie from a preceding signed host is not reused.
        try:
            with client.stream("GET", url) as response:
                if redirect and response.status_code == 302:
                    return response.headers
                if response.status_code != 200:
                    raise OSError(f"github_http_{response.status_code}")
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise OSError("provider_encoded_response")
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isdecimal() or int(length) > limit):
                    raise OSError("provider_declared_byte_limit")
                body = bytearray()
                for part in response.iter_raw(chunk_size=65536):
                    self._check()
                    self.received += len(part)
                    if self.received > 32 * MIB or len(body) + len(part) > limit:
                        raise OSError("provider_response_byte_limit")
                    body.extend(part)
                self._check()
                if length is not None and len(body) != int(length):
                    raise OSError("provider_length_mismatch")
                return bytes(body)
        except httpx.HTTPError:
            # Sanitized protocol error: never log bearer/temporary signed URLs.
            raise OSError("github_transport_failure") from None

    def _json(self, path):
        value = json.loads(self._request("https://api.github.com" + path), object_pairs_hook=_pairs)
        if not isinstance(value, dict):
            raise ValueError("provider_object_required")
        return value

    def _artifacts(self):
        rows, seen, total = [], set(), None
        for page in range(1, 11):
            value = self._json(f"/repos/{self.scope.repository}/actions/artifacts?per_page=100&page={page}")
            count, items = value.get("total_count"), value.get("artifacts")
            if (not _int(count) or count > 1000 or not isinstance(items, list) or len(items) > 100
                    or (total is not None and count != total)):
                raise ValueError("artifact_inventory_changed_or_limit")
            total = count
            for item in items:
                if not isinstance(item, dict) or not _int(item.get("id"), 1) or item["id"] in seen:
                    raise ValueError("artifact_duplicate_or_invalid_id")
                seen.add(item["id"])
                rows.append(item)
            if len(rows) == total:
                return rows
            if len(rows) > total or len(items) != 100:
                raise ValueError("artifact_inventory_incomplete")
        raise ValueError("artifact_inventory_incomplete")

    def _producer(self, item, repository_id, run_cache):
        summary = item.get("workflow_run")
        if not isinstance(summary, dict) or not _int(summary.get("id"), 1):
            raise ValueError("artifact_run_missing")
        run_id = summary["id"]
        if run_id not in run_cache:
            run_cache[run_id] = self._json(f"/repos/{self.scope.repository}/actions/runs/{run_id}")
        run = run_cache[run_id]
        if not _int(run.get("workflow_id"), 1):
            raise ValueError("artifact_workflow_id_invalid")
        if run.get("workflow_id") != self.scope.workflow_id:
            return None  # Identical prefix from another workflow is not our object.
        branch = self.scope.ref.removeprefix("refs/heads/")
        if (not _int(run.get("id"), 1) or run.get("id") != run_id
                or run.get("event") not in ("schedule", "workflow_dispatch")
                or run.get("head_branch") != branch or run.get("head_sha") not in self.allowed_sources
                or not isinstance(run.get("repository"), dict) or not isinstance(run.get("head_repository"), dict)
                or run.get("repository", {}).get("id") != repository_id
                or run.get("head_repository", {}).get("id") != repository_id
                or run.get("pull_requests") != []):
            raise ValueError("artifact_untrusted_producer")
        for field, expected in (("repository_id", repository_id), ("head_repository_id", repository_id),
                                ("head_branch", branch), ("head_sha", run["head_sha"])):
            if summary.get(field) != expected:
                raise ValueError("artifact_summary_mismatch")
        match = NAME.fullmatch(item["name"])
        if match is None:
            raise ValueError("artifact_name_invalid")
        generation, attempt = int(match[1]), int(match[2])
        if not _int(run.get("run_attempt"), 1) or attempt > run["run_attempt"]:
            raise ValueError("artifact_attempt_invalid")
        if attempt != run["run_attempt"]:
            key = (run_id, attempt)
            if key not in run_cache:
                run_cache[key] = self._json(f"/repos/{self.scope.repository}/actions/runs/{run_id}/attempts/{attempt}")
            previous = run_cache[key]
            if any(previous.get(k) != run.get(k) for k in
                   ("id", "workflow_id", "event", "head_branch", "head_sha", "repository", "head_repository", "pull_requests")):
                raise ValueError("artifact_attempt_producer_mismatch")
            run = previous
        if not _int(run.get("run_attempt"), 1) or run.get("run_attempt") != attempt:
            raise ValueError("artifact_attempt_invalid")
        return run, generation, attempt, match[3]

    def _download(self, artifact_id):
        headers = self._request(f"https://api.github.com/repos/{self.scope.repository}/actions/artifacts/{artifact_id}/zip",
                                redirect=True)
        if not isinstance(headers, httpx.Headers):
            raise ValueError("artifact_redirect_required")
        location = headers.get("Location", "")
        parsed = urlsplit(location)
        host = parsed.hostname or ""
        if (parsed.scheme != "https" or parsed.username is not None or parsed.password is not None
                or parsed.fragment or parsed.port not in (None, 443)
                or re.fullmatch(r"[a-z0-9.-]+", host) is None
                or not any(host.endswith("." + suffix) for suffix in
                           ("blob.core.windows.net", "actions.githubusercontent.com"))):
            raise ValueError("artifact_redirect_forbidden")
        return self._request(location, blob=True, limit=10 * MIB)

    def inventory(self) -> Inventory:
        self._check()
        # Clear all earlier observations if the new inventory cannot be proven.
        self.bodies.clear()
        repo = self._json(f"/repos/{self.scope.repository}")
        if (repo.get("full_name") != self.scope.repository or not _int(repo.get("id"), 1)
                or repo.get("default_branch") != self.scope.ref.removeprefix("refs/heads/")):
            raise ValueError("repository_identity_mismatch")
        first = self._artifacts()
        owned, run_cache = [], {}
        for item in first:
            if not isinstance(item.get("name"), str):
                raise ValueError("artifact_name_missing")
            if not item["name"].startswith(PREFIX):
                continue
            producer = self._producer(item, repo["id"], run_cache)
            if producer is not None:
                owned.append((item, producer))
                if len(owned) > 3:
                    raise ValueError("artifact_retention_backlog")
        records, bodies = [], {}
        for item, (run, generation, attempt, body_sha) in owned:
            size = item.get("size_in_bytes")
            created, expires = _timestamp(item.get("created_at")), _timestamp(item.get("expires_at"))
            archive_digest = item.get("digest", "")
            if (not _int(size, 1) or size > 10 * MIB or type(item.get("expired")) is not bool
                    or item["expired"] or created > self.now + 60000 or expires <= self.now
                    or not created < expires <= created + DAY + 60000
                    or not isinstance(archive_digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", archive_digest) is None):
                raise ValueError("artifact_size_expiry_or_digest")
            raw = self._download(item["id"])
            body = state_zip(raw, archive_digest[7:], body_sha)
            record, _ = decode_state_bundle(body, scope=self.scope, source_sha=run["head_sha"],
                                             run_id=run["id"], attempt=attempt)
            if (record.generation != generation or size < len(raw)
                    or record.created_at > self.now or not _timestamp(run.get("run_started_at")) - 60000
                    <= record.created_at <= created + 60000):
                raise ValueError("artifact_bundle_metadata_mismatch")
            actual_expiry = min(expires, record.created_at + DAY)
            if actual_expiry <= self.now:
                raise ValueError("artifact_expired")
            record = replace(record, object_id=str(item["id"]), size=size, expires_at=actual_expiry,
                             confirmed=True, validated=True)
            records.append(record)
            bodies[record.object_id] = body
        # All provider fields participate, not just names/counts; response order
        # is immaterial. No mutable remote global state is adopted as a lock.
        second = self._artifacts()
        if ({x["id"]: x for x in first} != {x["id"]: x for x in second}):
            raise ValueError("artifact_inventory_drift")
        final_repo = self._json(f"/repos/{self.scope.repository}")
        if any(final_repo.get(key) != repo.get(key) for key in ("id", "full_name", "default_branch")):
            raise ValueError("repository_identity_drift")
        self.bodies = bodies
        return Inventory(tuple(records), True, None, None, None, None)

    def read(self, object_id: str, limit: int) -> bytes:
        self._check()
        if object_id not in self.bodies:
            raise OSError("artifact_not_in_verified_inventory")
        body = self.bodies[object_id]
        if not _int(limit, 1) or len(body) > limit:
            raise ValueError("artifact_read_byte_limit")
        return body

    def upload(self, snapshot, body):
        raise PermissionError("read_only_artifact_adapter")

    def delete(self, object_id):
        raise PermissionError("read_only_artifact_adapter")
