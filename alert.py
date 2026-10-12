"""Portable local preview entry: uv run --locked --no-dev alert.py --help."""
import argparse
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'src'))

from crypto_boom import scan
from crypto_boom.runtime import Cancelled, DeadlineExceeded
from crypto_boom.state import StateStore, atomic_write
from crypto_boom.trial import Unavailable


def record(path):
    if not path.exists():
        return None
    with path.open('rb') as stream:
        raw = stream.read(4097)
    if len(raw) > 4096:
        raise Unavailable('invalid_schedule_record: recover only after confirming the old process stopped')
    value = json.loads(raw)
    if (not isinstance(value, dict) or set(value) != {'token', 'pid', 'interval_minutes'}
            or not isinstance(value['token'], str) or re.fullmatch('[0-9a-f]{32}', value['token']) is None
            or type(value['pid']) is not int or value['pid'] < 1
            or type(value['interval_minutes']) is not int or not 1 <= value['interval_minutes'] <= 1440):
        raise Unavailable('invalid_schedule_record')
    return value


def stopping(path, token):
    if not path.exists():
        return False
    with path.open('rb') as stream:
        if stream.read(33) != token.encode():
            raise Unavailable('foreign_stop_request: do not overwrite another instance')
    return True


def watch(workspace, args):
    workspace.mkdir(parents=True, exist_ok=True)
    owner, stop = workspace / 'schedule.json', workspace / 'stop.request'
    if stop.exists():
        raise Unavailable('stale_stop_request: use recover --confirm-stopped after the old process ends')
    token = uuid.uuid4().hex
    value = {'token': token, 'pid': os.getpid(), 'interval_minutes': args.interval}
    try:
        with owner.open('x', encoding='utf-8') as stream:
            json.dump(value, stream)
    except FileExistsError as exc:
        raise Unavailable('schedule_record_present: use status/stop, or recover after a reboot') from exc
    try:
        print(f'Local preview every {args.interval} minute(s); Ctrl+C or alert.py stop to cancel.', flush=True)
        while not stopping(stop, token):
            started = time.monotonic()
            try:
                scan.main(['--workspace', str(workspace), '--limit', str(args.limit), '--workers', str(args.workers)],
                          cancel=lambda: stopping(stop, token))
            except Cancelled:
                break
            except Unavailable as exc:
                # A reaped scan failure/deadline can be retried next tick; state/identity errors cannot.
                if (workspace / 'checkpoint.json.lock').exists() or not (
                        isinstance(exc, DeadlineExceeded) or str(exc).startswith('shadow_child_failed:')):
                    raise
                print(f'Scan failed; next scheduled attempt will reuse saved state: {exc}', file=sys.stderr, flush=True)
            due = started + args.interval * 60
            if due <= time.monotonic():
                due = time.monotonic() + args.interval * 60  # Skip missed ticks; never queue overlapping work.
            while time.monotonic() < due and not stopping(stop, token):
                time.sleep(min(.25, max(0, due - time.monotonic())))
    finally:
        # Do not advertise cancellation completion when worker death remains unknown.
        if not (workspace / 'checkpoint.json.lock').exists() and record(owner) == value:
            if stopping(stop, token):
                stop.unlink()
            owner.unlink()


def stop_watch(workspace):
    owner = workspace / 'schedule.json'
    value = record(owner)
    if value is None:
        print('No scheduled instance recorded. Ctrl+C cancels a foreground once run.')
        return
    request = workspace / 'stop.request'
    if not stopping(request, value['token']):
        atomic_write(request, value['token'].encode())
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        current = record(owner)
        if current is None or current['token'] != value['token']:
            print('Scheduled instance stopped; cache and ledger kept.')
            return
        time.sleep(.1)
    raise Unavailable('stop_requested_not_confirmed: after a reboot or confirmed process exit, use recover --confirm-stopped')


def recover(workspace):
    if not workspace.exists():
        print('No local workspace to recover.')
        return
    saved, reason = StateStore(workspace / 'checkpoint.json').load()
    if (saved is None and (workspace / 'checkpoint.json').exists()) or (saved and saved.notifications_enabled):
        raise Unavailable('refuse_recovery_of_invalid_or_armed_state:' + reason)
    # Explicit operator confirmation is the only authority to remove a stale lock.
    # No PID killing, auto-stealing, ledger reset or recursive deletion.
    for name in ('schedule.json', 'stop.request', 'checkpoint.json.lock'):
        (workspace / name).unlink(missing_ok=True)
    print('Stale local control files cleared; checkpoint, cache and ledger kept.')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, default=Path('.local-preview'), help='Project-relative persistent state directory')
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('once', 'watch'):
        command = commands.add_parser(name)
        command.add_argument('--limit', type=int, default=1000, choices=range(1, 1001), metavar='1..1000')
        command.add_argument('--workers', type=int, default=4, choices=(4, 8))
        if name == 'watch':
            command.add_argument('--interval', type=int, default=10, choices=range(1, 1441), metavar='MINUTES')
    commands.add_parser('stop')
    commands.add_parser('status')
    commands.add_parser('recover').add_argument('--confirm-stopped', action='store_true', required=True,
                                              help='Confirm all previous scan processes have ended; never use on a live process')
    args = parser.parse_args(argv)
    if args.workspace.anchor or '..' in args.workspace.parts:
        parser.error('--workspace must stay project-relative, without ..')
    os.chdir(ROOT)
    workspace = args.workspace
    try:
        for path in (workspace, *(workspace / name for name in
                     ('schedule.json', 'stop.request', 'checkpoint.json', 'checkpoint.json.lock'))):
            if path.is_symlink() or path.is_junction():
                raise Unavailable('local_workspace_link')
        if args.command == 'watch':
            watch(workspace, args)
        elif args.command == 'once':
            if (workspace / 'schedule.json').exists() or (workspace / 'stop.request').exists():
                raise Unavailable('scheduled_or_stale_control: use stop/recover before once')
            scan.main(['--workspace', str(workspace), '--limit', str(args.limit), '--workers', str(args.workers)])
        elif args.command == 'stop':
            stop_watch(workspace)
        elif args.command == 'recover':
            recover(workspace)
        else:
            value = record(workspace / 'schedule.json')
            print(json.dumps({'workspace': str(workspace), 'schedule_record': value,
                              'stop_requested': (workspace / 'stop.request').exists(),
                              'writer_lock_present': (workspace / 'checkpoint.json.lock').exists(),
                              'note': 'A record is not proof the process is alive; after a reboot, confirm exit before recovery.'}))
        return 0
    except KeyboardInterrupt:
        print('Cancelled; saved state kept.', file=sys.stderr)
        return 130
    except (OSError, ValueError) as exc:
        print(f'Local preview stopped: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
