"""Bounded default-branch observations via mock HTTP, NOT publication proof."""
import base64
import hashlib
from copy import deepcopy
from dataclasses import FrozenInstanceError

import httpx
import pytest
from test_okx_github import SCOPE, SOURCE, T, response

from crypto_boom import okx_github as github
from crypto_boom import okx_storage as storage
from crypto_boom.okx_state import Checkpoint
from crypto_boom.okx_trial import VERSION, Transition


def sha(value):
    return hashlib.sha1(value.encode()).hexdigest()


def marker(run=1):
    checkpoint = Checkpoint(generation=run, at=T, notifications_enabled=True)
    transition = Transition((VERSION + '/A0', ('okx', 'SPOT', 'ALT-USDT'), 'UP', T), 'OBSERVED', T)
    ids = checkpoint.prepare([transition], T)
    return storage.format_receipt_intent(checkpoint, ids, scope=SCOPE, source_sha=SOURCE, run_id=run, attempt=1)


class History:
    def __init__(self, contents):
        self.repo = {'id': 42, 'full_name': SCOPE.repository, 'default_branch': 'main'}
        self.commits, self.trees, self.blobs, self.requests = {}, {}, {}, []
        self.shas = [sha(f'commit-{i}') for i in range(len(contents))]
        self.anchor, self.head = self.shas[0], self.shas[-1]
        self.repo_count = self.ref_count = 0
        self.hook = None
        for i, raw in enumerate(contents):
            tree_sha = sha('tree-' + repr(raw))
            rows = []
            if raw is not None:
                blob_sha = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
                self.blobs[blob_sha] = {'sha': blob_sha, 'size': len(raw), 'encoding': 'base64',
                                       'content': base64.b64encode(raw).decode() + '\n'}
                rows = [{'path': 'README.md', 'mode': '100644', 'type': 'blob', 'sha': blob_sha}]
            self.trees[tree_sha] = {'sha': tree_sha, 'truncated': False, 'tree': rows}
            self.commits[self.shas[i]] = {'sha': self.shas[i], 'tree': {'sha': tree_sha},
                                         'parents': [{'sha': self.shas[i - 1]}] if i else [],
                                         # Untrusted dates deliberately nonsensical: never a cutoff.
                                         'author': {'date': '2099-01-01'}, 'committer': {'date': '1900-01-01'}}

    def handle(self, request):
        self.requests.append(request)
        assert request.method == 'GET' and request.url.host == 'api.github.com'
        assert not request.url.query
        assert 'Cookie' not in request.headers
        path = request.url.path
        if self.hook:
            changed = self.hook(self, path)
            if changed is not None:
                return changed
        prefix = f'/repos/{SCOPE.repository}'
        if path == prefix:
            self.repo_count += 1
            return response(self.repo)
        if path == prefix + '/git/ref/heads/main':
            self.ref_count += 1
            return response({'ref': SCOPE.ref, 'object': {'type': 'commit', 'sha': self.head}})
        for kind, values in [('commits', self.commits), ('trees', self.trees), ('blobs', self.blobs)]:
            if path.startswith(prefix + '/git/' + kind + '/'):
                value = values.get(path.rsplit('/', 1)[1])
                return response(value or b'missing', 200 if value else 404)
        raise AssertionError(path)

    def read(self, *, anchor=None, **kwargs):
        with github.GitHubStateReader(SCOPE, frozenset([SOURCE]), T,
                                     transport=httpx.MockTransport(self.handle), **kwargs) as reader:
            return reader.receipt_trace(self.anchor if anchor is None else anchor)


def test_carried_marker_replacement_removal_and_reintroduction_are_observed_once():
    a, b = marker().encode(), marker(2).encode()
    history = History([a, a, b, None, a])
    before = deepcopy(history.commits)
    trace = history.read(token='unit-test')
    assert trace == github.ReceiptTrace(SCOPE, history.anchor, history.head,
                                       ((history.shas[2], b.decode()), (history.shas[3], None),
                                        (history.shas[4], a.decode())))
    assert history.commits == before
    assert len(history.requests) == 14  # Immutable tree/blob content reused within observation.
    assert all(r.headers['Authorization'] == 'Bearer unit-test' for r in history.requests)
    assert not hasattr(trace, 'complete') and not hasattr(trace, 'published_at')
    with pytest.raises(FrozenInstanceError):
        trace.head = SOURCE
    with pytest.raises(AttributeError):
        trace.covers(SCOPE, T)  # Cannot masquerade as ReceiptQuery / recovery permission.


def test_anchor_marker_is_baseline_and_legacy_changes_are_not_okx_publications():
    a = marker().encode()
    assert History([a]).read().changes == ()
    assert History([a, b'# new heading\n' + a + b'\n']).read().changes == ()
    history = History([b'<!-- HOURLY_ALERT_RECEIPT_V1:e30= -->', b'# only old Binance data'])
    assert history.read().changes == ()
    changed = History([a, b'<!-- HOURLY_ALERT_RECEIPT_V1:e30= -->'])
    assert changed.read().changes == ((changed.head, None),)


def test_long_history_is_bounded_and_missing_anchor_never_returns_partial():
    boundary = History([f'# {i}'.encode() for i in range(8)])
    assert boundary.read().changes == ()
    assert len(boundary.requests) == 28
    longer = History([f'# {i}'.encode() for i in range(9)])
    with pytest.raises(ValueError, match='chain_limit'):
        longer.read()
    assert len(longer.requests) == 26 and longer.ref_count == 1
    missing = History([None, marker().encode()])
    with pytest.raises(ValueError, match='unreachable'):
        missing.read(anchor='f' * 40)
    # No date/Events endpoint, nor timestamps returned to fill Receipt.published_at.
    assert all('/events' not in str(r.url) for r in longer.requests + missing.requests)


@pytest.mark.parametrize('anchor', ['', 'x', 'A' * 40, None, 5, True])
def test_explicit_trusted_anchor_shape_is_required_before_http(anchor):
    history = History([None])
    with github.GitHubStateReader(SCOPE, frozenset([SOURCE]), T,
                                 transport=httpx.MockTransport(history.handle)) as reader:
        with pytest.raises(ValueError, match='anchor_required'):
            reader.receipt_trace(anchor)
    assert not history.requests


@pytest.mark.parametrize('case', ['merge', 'bad_commit', 'bad_tree_sha', 'bad_parent', 'cycle', 'missing_commit'])
def test_unusable_commit_graph_is_rejected(case):
    history = History([None, marker().encode()])
    commit = history.commits[history.head]
    if case == 'merge':
        commit['parents'].append({'sha': SOURCE})
    elif case == 'bad_commit':
        commit['sha'] = SOURCE
    elif case == 'bad_tree_sha':
        commit['tree']['sha'] = 'wrong'
    elif case == 'bad_parent':
        commit['parents'][0]['sha'] = True
    elif case == 'cycle':
        commit['parents'][0]['sha'] = history.head
    else:
        del history.commits[history.anchor]
    with pytest.raises((ValueError, OSError)):
        history.read()


@pytest.mark.parametrize('case', ['truncated', 'wrong_sha', 'rows_type', 'duplicate', 'nested_path', 'link',
                                  'submodule', 'bad_blob_sha', 'not_bool', 'too_many'])
def test_tree_completeness_and_regular_readme_are_required(case):
    history = History([marker().encode()])
    tree = next(iter(history.trees.values()))
    if case in ('truncated', 'not_bool'):
        tree['truncated'] = True if case == 'truncated' else 0
    elif case == 'wrong_sha':
        tree['sha'] = SOURCE
    elif case == 'rows_type':
        tree['tree'] = {}
    elif case == 'duplicate':
        tree['tree'] *= 2
    elif case == 'nested_path':
        tree['tree'][0]['path'] = 'folder/README.md'
    elif case == 'link':
        tree['tree'][0]['mode'] = '120000'
    elif case == 'submodule':
        tree['tree'][0]['type'] = 'commit'
    elif case == 'bad_blob_sha':
        tree['tree'][0]['sha'] = True
    else:
        tree['tree'] = [{'path': f'f{i}'} for i in range(10001)]
    with pytest.raises(ValueError):
        history.read()


@pytest.mark.parametrize('case', ['wrong_sha', 'wrong_hash', 'negative_size', 'bool_size', 'oversize',
                                  'size_mismatch', 'encoding', 'bad_base64', 'encoded_limit', 'padding_alias',
                                  'utf8', 'duplicate_marker'])
def test_blob_identity_bytes_and_extraction_are_bounded(case):
    raw = marker().encode()
    if case == 'utf8':
        raw = b'\xff'
    elif case == 'duplicate_marker':
        raw += b'\n' + raw
    history = History([raw])
    blob = next(iter(history.blobs.values()))
    changes = {'wrong_sha': ('sha', SOURCE), 'wrong_hash': ('content', base64.b64encode(b'x' * len(raw)).decode()),
               'negative_size': ('size', -1), 'bool_size': ('size', True),
               'oversize': ('size', storage.MIB + 1), 'size_mismatch': ('size', len(raw) + 1),
               'encoding': ('encoding', 'utf8'), 'bad_base64': ('content', '!bad!'),
               'encoded_limit': ('content', 'a' * (4 * ((storage.MIB + 2) // 3) + 1)),
               'padding_alias': ('content', blob['content'].replace('\n', '') + '=')}
    if case in changes:
        key, value = changes[case]
        blob[key] = value
    with pytest.raises(ValueError):
        history.read()


@pytest.mark.parametrize('kind', ['head', 'repo_id', 'branch', 'repo_name'])
def test_ref_and_repository_are_fenced_again_after_observation(kind):
    history = History([None, marker().encode()])
    def drift(provider, path):
        if path == f'/repos/{SCOPE.repository}' and provider.repo_count == 1:
            if kind == 'head':
                provider.head = SOURCE
            else:
                field = {'repo_id': 'id', 'branch': 'default_branch', 'repo_name': 'full_name'}[kind]
                provider.repo[field] = 99 if kind == 'repo_id' else 'different'
    history.hook = drift
    with pytest.raises(ValueError):
        history.read()


@pytest.mark.parametrize('case', ['ref_identity', 'ref_type', 'ref_sha', 'redirect', 'deadline', 'request_budget'])
def test_provider_failures_do_not_return_trace_or_retry(case):
    history = History([None])
    tick = [0]
    def fail(provider, path):
        if case == 'deadline':
            tick[0] = 121
        if path.endswith('/git/ref/heads/main'):
            ref = {'ref': SCOPE.ref, 'object': {'type': 'commit', 'sha': history.head}}
            if case == 'ref_identity':
                ref['ref'] = 'refs/heads/foreign'
            elif case == 'ref_type':
                ref['object']['type'] = 'tag'
            elif case == 'ref_sha':
                ref['object']['sha'] = None
            elif case == 'redirect':
                return response(b'', 302, {'Location': 'https://evil.invalid'})
            else:
                return None
            return response(ref)
    history.hook = fail
    with github.GitHubStateReader(SCOPE, frozenset([SOURCE]), T, clock=lambda: tick[0],
                                 transport=httpx.MockTransport(history.handle)) as reader:
        if case == 'request_budget':
            reader.requests = 31
        with pytest.raises((ValueError, OSError)):
            reader.receipt_trace(history.anchor)
    assert len(history.requests) <= 2


def test_large_regular_blob_boundary_and_closed_reader():
    history = History([b'x' * storage.MIB])
    assert history.read().changes == ()
    with github.GitHubStateReader(SCOPE, frozenset([SOURCE]), T,
                                 transport=httpx.MockTransport(history.handle)) as reader:
        pass
    count = len(history.requests)
    with pytest.raises(OSError, match='closed_or_deadline'):
        reader.receipt_trace(history.anchor)
    assert len(history.requests) == count


def test_extracted_claim_requires_separate_wire_producer_and_time_validation():
    invalid_claim = '<!-- OKX_ALERT_RECEIPT_V1:abc -->'
    history = History([None, invalid_claim.encode()])
    trace = history.read()
    assert trace.changes == ((history.head, invalid_claim),)
    with pytest.raises(ValueError):
        storage.parse_public_receipt(trace.changes[0][1], scope=SCOPE, source_sha=SOURCE,
                                     run_id=1, attempt=1, published_at=T)
    # An untrusted marker is visible evidence to diagnose, never quietly confirmed.
    assert not isinstance(trace, storage.ReceiptQuery)


def test_second_observation_refetches_heads_and_shares_existing_budget_without_state_changes():
    a = marker().encode()
    history = History([None, a])
    with github.GitHubStateReader(SCOPE, frozenset([SOURCE]), T,
                                 transport=httpx.MockTransport(history.handle)) as reader:
        reader.bodies['unrelated-valid-artifact'] = b'preserve'
        first = reader.receipt_trace(history.anchor)
        count = reader.requests
        history.head = history.anchor
        second = reader.receipt_trace(history.anchor)
        assert first.changes and not second.changes
        assert second.head == history.anchor and reader.requests > count
        assert history.ref_count == history.repo_count == 4
        assert reader.bodies == {'unrelated-valid-artifact': b'preserve'}
