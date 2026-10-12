"""Retirement and source identity invariants, without GitHub effects."""
from pathlib import Path

from crypto_boom import cache

ROOT = Path(__file__).resolve().parents[1]


def test_retired_files_and_python_prefixes_have_no_surviving_owner():
    assert not list((ROOT / 'src/crypto_boom').glob('okx_*.py'))
    assert not list((ROOT / 'tests').glob('test_okx_*.py'))
    assert not (ROOT / 'src/crypto_boom/hourly_alert.py').exists()
    assert {p.name for p in (ROOT / '.github/workflows').glob('*.yml')} == {'ci.yml', 'okx-live.yml'}
    retired = ('crypto_boom.okx_', 'test_okx_', 'hourly-altcoin-alert.yml', 'legacy.json')
    for folder in ('src', '.github/workflows'):
        for path in (ROOT / folder).rglob('*'):
            if path.is_file() and path.suffix in ('.py', '.yml'):
                text = path.read_text(encoding='utf-8')
                assert not any(name in text for name in retired), path


def test_code_digest_covers_renamed_modules_contents_names_and_lf(tmp_path, monkeypatch):
    entry = tmp_path / 'cache.py'
    entry.write_bytes(b'# cache\n')
    peer = tmp_path / 'state.py'
    peer.write_bytes(b'# state\n')
    monkeypatch.setattr(cache, '__file__', str(entry))
    first = cache.code_digest()
    assert first != cache.digest(cache._json({}))
    peer.write_bytes(b'# state\r\n')
    assert cache.code_digest() == first
    peer.write_bytes(b'# changed\n')
    changed = cache.code_digest()
    assert changed != first
    peer.rename(tmp_path / 'other.py')
    assert cache.code_digest() != changed


def test_current_readme_has_one_public_receipt_and_no_retired_hourly_status():
    from crypto_boom.storage import RECEIPT_TAG, extract_public_receipt

    raw = (ROOT / 'README.md').read_bytes()
    assert raw.count(RECEIPT_TAG.encode()) == 1
    assert extract_public_receipt(raw) is not None
    assert b'HOURLY_ALERT_STATUS_' not in raw
    assert b'crypto_boom.hourly_alert' not in raw
