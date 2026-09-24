# Sync activities run in the worker's thread pool, and an activity's start_to_close
# timer starts when the worker accepts the task, not when a thread picks it up.

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from pipeline import worker


def test_the_worker_accepts_no_more_activities_than_it_has_threads(monkeypatch):
    """Worker defaults to 100 concurrent activities. With 16 threads, the other 84
    wait in the executor queue while their timeouts run, and a large document's
    embed batches can time out without ever having started."""
    seen = {}
    monkeypatch.setattr(worker, "Worker", lambda client, **kw: seen.update(kw))

    with ThreadPoolExecutor(max_workers=worker.ACTIVITY_THREADS) as executor:
        worker.build_worker(object(), executor)

    assert seen["activity_executor"] is executor
    assert seen["max_concurrent_activities"] == worker.ACTIVITY_THREADS
