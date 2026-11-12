"""Concurrent data collection with per-source retries (Python 3.11+)."""
import asyncio
import random
from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

T = TypeVar('T')

class TransientSourceError(Exception):
    """A source failure that may resolve on a later attempt."""

class InvalidRecordError(Exception):
    """A permanent data validation failure."""

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

async def retry_operation(
    operation: Callable[[], Awaitable[T]],
    semaphore: asyncio.Semaphore,
    *,
    max_attempts: int = 4,
    attempt_timeout: float = 1.0,
    base_delay: float = 0.01,
    max_delay: float = 0.5,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    jitter: Callable[[float, float], float] = random.uniform,
) -> tuple[T, int]:
    """Retry transient failures, holding a concurrency slot only during I/O."""
    if max_attempts < 1 or attempt_timeout <= 0 or base_delay < 0 or max_delay < 0:
        raise ValueError('Invalid retry configuration')
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
    raise AssertionError('Unreachable')

async def collect_sources(
    sources: list[Source], *, max_concurrency: int = 3,
    max_attempts: int = 4, attempt_timeout: float = 1.0,
    base_delay: float = 0.01,
) -> list[Outcome]:
    if max_concurrency < 1:
        raise ValueError('max_concurrency must be positive')
    semaphore = asyncio.Semaphore(max_concurrency)

    async def one(source: Source) -> Outcome:
        attempts = 0
        async def fetch() -> int:
            nonlocal attempts
            attempts += 1
            await asyncio.sleep(source.delay)
            if source.invalid:
                raise InvalidRecordError('invalid source record')
            if attempts <= source.transient_failures:
                raise TransientSourceError('temporary upstream failure')
            return source.value
        try:
            value, _ = await retry_operation(
                fetch, semaphore, max_attempts=max_attempts,
                attempt_timeout=attempt_timeout, base_delay=base_delay,
            )
            return Outcome(source.name, value, attempts)
        except (TransientSourceError, TimeoutError, InvalidRecordError) as exc:
            return Outcome(source.name, None, attempts, type(exc).__name__)

    # TaskGroup propagates unexpected programming errors and cancellation.
    tasks: list[asyncio.Task[Outcome]] = []
    async with asyncio.TaskGroup() as group:
        for source in sources:
            tasks.append(group.create_task(one(source)))
    return [task.result() for task in tasks]

async def main() -> None:
    sources = [
        Source('orders', 120),
        Source('inventory', 85),
        Source('payments', 110, transient_failures=2),
        Source('shipments', 95, transient_failures=5),
        Source('returns', 12, invalid=True),
    ]
    results = await collect_sources(sources, base_delay=0)
    for item in results:
        status = f'OK value={item.value}' if item.error is None else f'FAILED {item.error}'
        print(f'{item.name}: {status}; attempts={item.attempts}')
    print('Total:', sum(item.value for item in results if item.value is not None))

if __name__ == '__main__':
    asyncio.run(main())
