"""Standard-library asynchronous tests: python -m unittest -v."""
import asyncio
import unittest
from collector import (
    Source, TransientSourceError, InvalidRecordError,
    collect_sources, retry_operation,
)

class CollectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_only_failed_source(self):
        results = await collect_sources([
            Source('good', 5), Source('flaky', 7, transient_failures=2)
        ], base_delay=0)
        self.assertEqual([(r.value, r.attempts) for r in results], [(5, 1), (7, 3)])

    async def test_exhausted_retries_do_not_discard_successes(self):
        results = await collect_sources([
            Source('good', 10), Source('down', 4, transient_failures=9)
        ], base_delay=0)
        self.assertEqual(results[0].value, 10)
        self.assertEqual(results[1].error, 'TransientSourceError')
        self.assertEqual(results[1].attempts, 4)

    async def test_invalid_data_is_not_retried(self):
        (result,) = await collect_sources([Source('bad', 3, invalid=True)], base_delay=0)
        self.assertEqual((result.error, result.attempts), ('InvalidRecordError', 1))

    async def test_timeout_is_retried(self):
        (result,) = await collect_sources(
            [Source('slow', 2, delay=0.02)], attempt_timeout=0.001,
            base_delay=0,
        )
        self.assertEqual((result.error, result.attempts), ('TimeoutError', 4))

    async def test_semaphore_limits_concurrent_attempts(self):
        active = 0
        peak = 0
        semaphore = asyncio.Semaphore(2)
        async def operation():
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.01)
                return 1
            finally:
                active -= 1
        await asyncio.gather(*[
            retry_operation(operation, semaphore) for _ in range(8)
        ])
        self.assertEqual(peak, 2)

    async def test_backoff_releases_semaphore(self):
        semaphore = asyncio.Semaphore(1)
        waiting = asyncio.Event()
        release = asyncio.Event()
        attempts = 0
        async def operation():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TransientSourceError()
            return 42
        async def fake_sleep(delay):
            waiting.set()
            await release.wait()
        task = asyncio.create_task(retry_operation(
            operation, semaphore, sleep=fake_sleep, jitter=lambda a, b: b,
        ))
        await asyncio.wait_for(waiting.wait(), timeout=1)
        self.assertFalse(semaphore.locked())
        release.set()
        self.assertEqual(await task, (42, 2))

    async def test_cancellation_propagates(self):
        entered = asyncio.Event()
        async def operation():
            entered.set()
            await asyncio.sleep(10)
        task = asyncio.create_task(retry_operation(operation, asyncio.Semaphore(1)))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_configuration_validation(self):
        async def operation():
            return 1
        with self.assertRaises(ValueError):
            await retry_operation(operation, asyncio.Semaphore(1), max_attempts=0)

if __name__ == '__main__':
    unittest.main()
