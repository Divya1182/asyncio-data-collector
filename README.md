# Build a Fault-Tolerant Data Collector With Python Asyncio

*A practical tutorial for intermediate Python developers · Python 3.11+*

When an application collects data from several independent services, a single slow or unavailable service shouldn't force the application to repeat every successful request. In this tutorial, you'll build a concurrent collector that retries **only the failing source**, enforces timeouts, limits in-flight operations, and returns a result for every source.

The collector uses simulated upstream sources so you can reproduce every outcome without network access or API credentials. You'll also write tests for failure recovery, concurrency, and cancellation. By the end, you'll understand not just how to use `asyncio`, but why the placement of a semaphore or timeout changes your application's behavior.

## What you'll learn

- Schedule independent I/O-bound operations concurrently with `asyncio.TaskGroup`.
- Restrict simultaneous attempts with `asyncio.Semaphore`.
- Retry transient failures with bounded exponential backoff and jitter.
- Distinguish transient failures from permanent validation errors.
- Preserve successful results when one source exhausts its retries.
- Test async code using `unittest.IsolatedAsyncioTestCase`.

**Prerequisites:** Comfortable with Python functions, exceptions, type hints, and basic `async`/`await`. You need Python **3.11 or newer**. All code uses the standard library.

## Set up the project

Clone or download this repository, then run:

```bash
python --version
python collector.py
python -m unittest discover -s tests -v
```

The project layout is:

```text
asyncio-data-collector/
├── README.md
├── collector.py
└── tests/
    ├── __init__.py
    └── test_collector.py
```

No package installation is necessary.

## 1. Model each source and its outcome

Start with immutable dataclasses. A `Source` describes one upstream operation. An `Outcome` captures its result and the number of attempts made:

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Source:
    name: str
    value: int
    delay: float = 0.01
    transient_failures: int = 0
    invalid: bool = False

@dataclass(frozen=True)
class Outcome:
    name: str
    value: int | None
    attempts: int
    error: str | None = None
```

The `transient_failures` and `invalid` fields simulate upstream behavior; they aren't a suggested real API schema. In a real application, the collector would use an HTTP client or database adapter and parse actual responses.

Using `None` for an unsuccessful value is deliberate: zero may be a valid collected value, so checking `if value` would incorrectly discard it.

## 2. Retry the individual operation, not the whole batch

Transient failures include temporary connectivity problems and timeouts. A malformed record, by contrast, won't improve if you repeat the same operation. Represent these cases with separate exceptions:

```python
class TransientSourceError(Exception):
    """A source failure that may resolve on a later attempt."""

class InvalidRecordError(Exception):
    """A permanent data validation failure."""
```

The `retry_operation()` function in [`collector.py`](collector.py) accepts an async callable, a shared semaphore, and retry settings. Its core is:

```python
for attempt in range(1, max_attempts + 1):
    try:
        async with semaphore:
            async with asyncio.timeout(attempt_timeout):
                result = await operation()
        return result, attempt
    except (TransientSourceError, TimeoutError):
        if attempt == max_attempts:
            raise
        ceiling = min(max_delay, base_delay * 2 ** (attempt - 1))
        await sleep(jitter(0, ceiling))
```

**Attempt count matters:** `max_attempts=4` means **one initial attempt plus at most three retries**, not four retries. After the fourth failed attempt, the last exception is raised.

The timeout is *per attempt*, not an overall deadline for the complete operation. With backoff, a failing source may take longer than `max_attempts × attempt_timeout`. A real service may also need an overall request deadline.

### Why release the semaphore before backoff?

Notice the `await sleep(...)` call occurs **outside** `async with semaphore`. Waiting for the next retry should not occupy a scarce concurrency slot. Otherwise, three sleeping operations could prevent a healthy fourth source from making progress.

`asyncio.sleep()` cooperatively yields control to the event loop. Calling `time.sleep()` here would block the event-loop thread and delay unrelated tasks.

### Why jitter?

A basic exponential schedule with a 0.5-second base delay produces ceilings of 0.5, 1, 2, and 4 seconds for successive retries (subject to the configured cap). The implementation uses **full jitter**: each actual delay is randomly selected between zero and that ceiling. This reduces the chance that many clients retry at exactly the same time.

Jitter also means elapsed time varies from run to run. Tests should assert observable behavior, not exact wall-clock timing.

## 3. Collect sources concurrently

`collect_sources()` creates one coroutine per source and runs them in a `TaskGroup`:

```python
semaphore = asyncio.Semaphore(max_concurrency)

tasks = []
async with asyncio.TaskGroup() as group:
    for source in sources:
        tasks.append(group.create_task(one(source)))

return [task.result() for task in tasks]
```

Each `one()` coroutine maintains its own attempt counter and retries only its own `fetch()` operation. Expected upstream failures are converted into `Outcome` objects; successes remain available even when a different source fails permanently or exhausts its attempts.

Why `TaskGroup` instead of `gather()`? Structured concurrency ensures that child tasks are managed within a defined scope. If a child raises an **unexpected** exception, `TaskGroup` cancels the other children and propagates an exception group. That's desirable for programming errors that should not be silently converted into routine upstream failures. We intentionally catch only the source failures our application knows how to report.

Cancellation is also important. We do **not** catch `asyncio.CancelledError` in our retry loop, so shutdown and parent-task cancellation can propagate. Cleanup, if required, belongs in `finally` blocks.

## 4. Run the demonstration

The sample includes five sources:

```python
sources = [
    Source('orders', 120),
    Source('inventory', 85),
    Source('payments', 110, transient_failures=2),
    Source('shipments', 95, transient_failures=5),
    Source('returns', 12, invalid=True),
]
```

Run `python collector.py`. Expected output:

```text
orders: OK value=120; attempts=1
inventory: OK value=85; attempts=1
payments: OK value=110; attempts=3
shipments: FAILED TransientSourceError; attempts=4
returns: FAILED InvalidRecordError; attempts=1
Total: 315
```

`payments` succeeds on its third attempt. `shipments` fails all four allowed attempts. `returns` fails immediately because invalid data is not retried. Yet the successful results are still aggregated: **120 + 85 + 110 = 315**.

The demo sets the base backoff delay to zero to make it quick and deterministic. The reusable retry function defaults to nonzero backoff; in production you should configure retry delays and budgets for the upstream service's limits.

## 5. Test the behaviors that matter

The test suite uses `unittest.IsolatedAsyncioTestCase`, which runs each async test in its own event loop. Run:

```bash
python -m unittest discover -s tests -v
```

The tests cover:

1. A healthy source runs once while a flaky source retries independently.
2. Exhausting retries doesn't erase successful outcomes.
3. Invalid data is not retried.
4. Timeouts count toward the retry limit.
5. A semaphore enforces the maximum number of active attempts.
6. A retrying task releases its semaphore slot during backoff.
7. Cancellation propagates instead of being retried.
8. Invalid configuration is rejected.

One revealing test injects a custom `sleep` function that pauses a retry during backoff. While the retry is paused, it checks that the semaphore is **not locked**. This tests an architectural property directly, rather than assuming the code is correct because it finishes quickly.

## 6. Understand the tradeoffs

**Individual retries vs. batch retries.** Retrying only failed sources avoids repeating successful work. The tradeoff is more state: you must track attempts and outcomes for each source and decide how to handle partially successful batches.

**Retries vs. duplicate writes.** Our example simulates read-only collection. Retrying writes can be dangerous: a server might commit a transaction before the client times out. A subsequent retry could duplicate that transaction. Use idempotency keys, deduplication, or transactional guarantees when retries affect persistent state.

**Concurrency vs. upstream load.** More simultaneous tasks can reduce latency for I/O-bound workloads, but unlimited concurrency may overwhelm a dependency. The semaphore bounds in-flight attempts; it is **not** a rate limiter and does not guarantee a fixed number of requests per second.

**Timeouts vs. overall deadlines.** A per-attempt timeout prevents an individual attempt from waiting indefinitely. An overall deadline is still useful to bound the entire retry process, including backoff and queueing.

**Failure classification.** In a real HTTP collector, status codes like 429 and 503 may be retryable, but 400 or 401 usually aren't. Respect `Retry-After` where applicable, and don't blindly retry every exception. Network requests can fail in ways that require more careful classification than this simulated example.

## 7. Where to go next

Try these extensions:

- Replace simulated sources with an async HTTP client and handle real status codes.
- Add a global deadline around the entire batch.
- Emit structured logs with source name, attempt count, and error type.
- Add an idempotent persistence layer for successful results.
- Test what happens when the parent collector task is cancelled midway through processing.

## Conclusion

Reliable asynchronous data collection isn't just about starting several coroutines at once. It requires deliberate decisions about which failures to retry, how long to wait, how many requests to run concurrently, and what to do when only part of the batch succeeds.

In this project, `TaskGroup` gives tasks a clear lifetime, the semaphore controls active work, and per-source retry logic preserves successful results without repeating the entire batch. The tests demonstrate that these behaviors hold even under timeouts and cancellation.

### References

- [Python asyncio documentation](https://docs.python.org/3/library/asyncio.html)
- [Python Task Groups](https://docs.python.org/3/library/asyncio-task.html#task-groups)
- [Python asyncio synchronization primitives](https://docs.python.org/3/library/asyncio-sync.html)
- [Python IsolatedAsyncioTestCase](https://docs.python.org/3/library/unittest.html#unittest.IsolatedAsyncioTestCase)

---

*Editorial note for the prospective author: This is an AI-assisted draft for technical review and adaptation. Run the tests, verify the behavior, and revise the prose with your own explanations and experience before submitting it as a personal writing sample. Check the publisher's AI-assistance rules.*
