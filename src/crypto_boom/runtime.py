"""Own one disposable job process, including blocking worker/commit cleanup.

No multiprocessing queue, pipe, or child-finally acknowledgement is required.
Only local no-publish jobs use this owner; this is not a scheduling service.
"""
from __future__ import annotations

import math
import multiprocessing
import time

from .trial import Unavailable


class DeadlineExceeded(Unavailable):
    pass


class UnreapedChild(Unavailable):
    pass


def supervise(target, args=(), *, seconds=480, grace=5, lock_owner=None):
    """Terminate before the outer deadline; release a lock only after reaping.

    Native process creation/OS/filesystem failures are outside a strict wall-time
    SLA. The owned target may use threads but must not create child processes.
    """
    if not (math.isfinite(seconds) and math.isfinite(grace) and 0 < grace < seconds <= 600):
        raise ValueError("Invalid supervision window")
    deadline = time.monotonic() + seconds
    process = multiprocessing.get_context("spawn").Process(target=target, args=args, daemon=True)
    # A failed/interrupted native start can leave its outcome unobservable.
    # Once the loan can escape, only confirmed process death releases the lock.
    if lock_owner is not None:
        lock_owner.retain_lock = True
    try:
        process.start()
        process.join(max(0, deadline - grace - time.monotonic()))
        if process.is_alive():
            raise DeadlineExceeded("job_deadline: reload checkpoint; commit outcome may be old or new")
        return process.exitcode
    finally:
        if process.pid is not None:
            # Fail closed even if terminate/join themselves raise an OS error.
            if lock_owner is not None:
                lock_owner.retain_lock = True
            if process.is_alive():
                process.terminate()
                process.join(grace / 2)
            if process.is_alive():
                process.kill()
                process.join(grace / 2)
            if process.is_alive():
                if lock_owner is not None:
                    lock_owner.retain_lock = True
                raise UnreapedChild("child_not_reaped: retain writer lock")
            if lock_owner is not None:
                lock_owner.retain_lock = False
            process.close()
