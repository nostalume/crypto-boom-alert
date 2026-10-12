"""Native cache scope, byte digests and bounded public alert receipts.

Receipt parsing is not provider ancestry or delivery proof. The live workflow
owns authenticated run/cache metadata; the publisher owns commit readback.
"""
import base64
import hashlib
import json
import re
from dataclasses import asdict, dataclass

from crypto_boom.state import Checkpoint
from crypto_boom.trial import HOUR, MINUTE, VERSION

MIB = 1024 * 1024
GIB = 1024 * MIB
DAY = 24 * HOUR
RECEIPT_WIRE = "okx-public-receipt-v1"
RECEIPT_TAG = "OKX_ALERT_RECEIPT_V1"
RECEIPT_PAYLOAD_LIMIT = 768 * 1024
RECEIPT_MARKER = re.compile(r"<!-- OKX_ALERT_RECEIPT_V1:([A-Za-z0-9_-]+={0,2}) -->")


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


def _receipt_payload(body):
    fields = {"schema", "rules_version", "scope", "source_sha", "run_id", "attempt",
              "generation", "prepared_at", "ids", "batch"}
    if not isinstance(body, dict) or set(body) != fields:
        raise ValueError("public_receipt_fields")
    ids = body["ids"]
    if (body["schema"] != RECEIPT_WIRE or body["rules_version"] != VERSION
            or not isinstance(body["scope"], dict) or set(body["scope"]) != {"repository", "workflow_id", "ref"}
            or not _hex(body["source_sha"], 40) or not _integer(body["run_id"], 1)
            or not _integer(body["attempt"], 1) or not _integer(body["generation"], 1)
            or not _integer(body["prepared_at"]) or body["prepared_at"] % MINUTE
            or not isinstance(ids, list) or not 0 < len(ids) <= 10000
            or any(not _hex(key) for key in ids) or ids != sorted(set(ids))
            or not _hex(body["batch"]) or body["batch"] != digest(_json(ids))):
        raise ValueError("public_receipt_identity")
    Scope(**body["scope"])


def format_receipt_intent(checkpoint: Checkpoint, ids: tuple[str, ...], *, scope: Scope,
                          source_sha: str, run_id: int, attempt: int) -> str:
    """Serialize one full saved intent identity; no publication or save proof.

    All ledger statuses serialize identically for offline reconciliation. The
    sender must separately require durable pending and refuse unknown retries.
    This function cannot enable notifications or confirm a ledger.
    """
    if (not isinstance(checkpoint, Checkpoint) or not isinstance(scope, Scope)
            or not isinstance(ids, tuple) or not 0 < len(ids) <= 10000
            or any(not _hex(key) for key in ids) or len(set(ids)) != len(ids)):
        raise ValueError("invalid_receipt_intent")
    try:
        checked = Checkpoint.decode(checkpoint.encode())
    except (ValueError, KeyError, TypeError, OverflowError) as exc:
        raise ValueError("invalid_receipt_checkpoint") from exc
    ordered = tuple(sorted(ids))
    batch = digest(_json(ordered))
    if (any(key not in checked.ledger or checked.ledger[key]["batch"] != batch for key in ordered)
            or {key for key, item in checked.ledger.items() if item["batch"] == batch} != set(ordered)):
        raise ValueError("receipt_requires_whole_saved_batch")
    prepared = {checked.ledger[key]["prepared_at"] for key in ordered}
    if len(prepared) != 1:
        raise ValueError("receipt_batch_prepare_time")
    body = {"schema": RECEIPT_WIRE, "rules_version": VERSION, "scope": asdict(scope),
            "source_sha": source_sha, "run_id": run_id, "attempt": attempt,
            "generation": checked.generation, "prepared_at": prepared.pop(),
            "ids": list(ordered), "batch": batch}
    _receipt_payload(body)
    raw = _json(body)
    if len(raw) > RECEIPT_PAYLOAD_LIMIT:
        raise ValueError("public_receipt_payload_limit")
    marker = f"<!-- {RECEIPT_TAG}:{base64.urlsafe_b64encode(raw).decode('ascii')} -->"
    if len(marker) > MIB:
        raise ValueError("public_receipt_marker_limit")
    return marker


def parse_public_receipt(marker: str, *, scope: Scope, source_sha: str, run_id: int,
                         attempt: int, published_at: int) -> Receipt:
    """Bind wire claims to caller-verified provider identity and commit time.

    The caller must verify accepted default-branch ancestry and run provenance
    independently. Parsing proves neither that ancestry nor query completeness.
    published_at is supplied by that provider, never by embedded JSON.
    """
    if (not isinstance(scope, Scope) or not _hex(source_sha, 40) or not _integer(run_id, 1)
            or not _integer(attempt, 1) or not _integer(published_at)):
        raise ValueError("invalid_receipt_authority")
    if not isinstance(marker, str) or not 0 < len(marker) <= MIB:
        raise ValueError("public_receipt_marker_limit")
    match = RECEIPT_MARKER.fullmatch(marker)
    if match is None:
        raise ValueError("public_receipt_marker")
    encoded = match[1]
    raw = base64.b64decode(encoded, altchars=b"-_", validate=True)
    if not 0 < len(raw) <= RECEIPT_PAYLOAD_LIMIT or base64.urlsafe_b64encode(raw).decode('ascii') != encoded:
        raise ValueError("public_receipt_noncanonical_or_limit")
    try:
        body = json.loads(raw.decode('utf-8'), object_pairs_hook=_strict_object)
        _receipt_payload(body)
    except (TypeError, KeyError, RecursionError) as exc:
        raise ValueError("public_receipt_invalid_payload") from exc
    if (Scope(**body["scope"]) != scope or body["source_sha"] != source_sha
            or body["run_id"] != run_id or body["attempt"] != attempt
            or published_at < body["prepared_at"]):
        raise ValueError("public_receipt_provider_mismatch")
    return Receipt(scope, body["generation"], published_at, tuple(body["ids"]), body["batch"])


def extract_public_receipt(readme: bytes) -> str | None:
    """Read one reserved, standalone marker from bounded UTF-8 README bytes.

    Legacy Binance and marker-free content return None, not an OKX receipt. A
    duplicate or malformed reserved tag is diagnostic, never silently skipped.
    """
    if not isinstance(readme, bytes) or len(readme) > MIB:
        raise ValueError("public_receipt_readme_limit")
    text = readme.decode('utf-8')
    count = text.count(RECEIPT_TAG)
    if not count:
        return None
    if count != 1:
        raise ValueError("public_receipt_duplicate_marker")
    line = next(line for line in text.splitlines() if RECEIPT_TAG in line)
    if RECEIPT_MARKER.fullmatch(line) is None:
        raise ValueError("public_receipt_marker")
    return line


def _strict_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate_json_key")
        value[key] = item
    return value
