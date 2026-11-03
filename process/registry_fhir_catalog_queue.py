# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep an unbounded catalog job inside a finite, owned ARQ claim."""

import asyncio
from dataclasses import dataclass
from uuid import uuid4

from arq.constants import abort_jobs_ss, in_progress_key_prefix, job_key_prefix, result_key_prefix, retry_key_prefix
from arq.utils import timestamp_ms, to_ms
from arq.worker import Worker, get_kwargs
from redis.exceptions import WatchError

CLAIM_SECONDS = 60
RENEW_SECONDS = 15
REDIS_SECONDS = 5

_RENEW = """
if redis.call('get', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('pexpire', KEYS[1], ARGV[2])
for i = 2, 3 do
    local ttl = redis.call('pttl', KEYS[i])
    if ttl >= 0 and ttl < tonumber(ARGV[2]) then redis.call('pexpire', KEYS[i], ARGV[2]) end
end
return 1
"""


@dataclass
class _Claim:
    token: bytes
    deadline: float
    finalizing: bool = False


class FHIRCatalogWorker(Worker):
    """Use ARQ execution and results with renewable, fenced claim ownership."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if (
            self.max_jobs != 1
            or self.max_tries != 1
            or self.job_timeout_s is not None
            or any(function.timeout_s is not None for function in self.functions.values())
        ):
            raise ValueError("registry_fhir_catalog_queue_settings_invalid")
        self._claims = {}

    async def start_jobs(self, job_ids):
        """Start eligible jobs only after acquiring their finite queue claims."""
        for job_id_bytes in job_ids:
            await self.sem.acquire()
            if self.job_counter >= self.max_jobs:
                self.sem.release()
                return
            self.job_counter += 1
            job_id = job_id_bytes.decode()
            claim = _Claim(uuid4().hex.encode(), self.loop.time() + CLAIM_SECONDS)
            key = in_progress_key_prefix + job_id
            has_started = False
            try:
                async with asyncio.timeout(REDIS_SECONDS), self.pool.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    occupied = await pipe.exists(key)
                    score = await pipe.zscore(self.queue_name, job_id)
                    if occupied or score is None or score > timestamp_ms():
                        continue
                    pipe.multi()
                    pipe.psetex(key, to_ms(CLAIM_SECONDS), claim.token)
                    await pipe.execute()
                self._claims[job_id] = claim
                task = self.loop.create_task(self.run_job(job_id, int(score)))
                task.add_done_callback(lambda _: self._release_sem_dec_counter_on_complete())
                self.tasks[job_id] = task
                has_started = True
            except WatchError:
                continue
            finally:
                if not has_started:
                    self._release_sem_dec_counter_on_complete()

    async def _renew_claim(self, job_id, claim):
        started = self.loop.time()
        if started >= claim.deadline:
            raise RuntimeError("registry_fhir_catalog_queue_claim_lost")
        async with asyncio.timeout_at(min(claim.deadline, started + REDIS_SECONDS)):
            renewed = await self.pool.eval(
                _RENEW,
                3,
                in_progress_key_prefix + job_id,
                job_key_prefix + job_id,
                retry_key_prefix + job_id,
                claim.token,
                to_ms(CLAIM_SECONDS),
            )
        if not renewed:
            raise RuntimeError("registry_fhir_catalog_queue_claim_lost")
        claim.deadline = started + CLAIM_SECONDS

    async def _keep_claim(self, job_id, claim, owner):
        try:
            while not claim.finalizing:
                async with asyncio.timeout_at(claim.deadline):
                    await asyncio.sleep(RENEW_SECONDS)
                    if claim.finalizing:
                        return
                    await self._renew_claim(job_id, claim)
        except Exception:
            if not claim.finalizing:
                owner.cancel()

    async def run_job(self, job_id, score):
        """Run ARQ execution while renewing ownership and drain renewal on exit."""
        claim = self._claims[job_id]
        renewal = None
        try:
            try:
                await self._renew_claim(job_id, claim)
            except Exception:
                self.jobs_failed += 1
                return
            renewal = asyncio.create_task(self._keep_claim(job_id, claim, asyncio.current_task()))
            await super().run_job(job_id, score)
        finally:
            if renewal is not None:
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
            self._claims.pop(job_id, None)

    async def finish_job(
        self, job_id, finish, result_data, result_timeout_s, keep_result_forever, incr_score, keep_in_progress
    ):
        """Apply ARQ result cleanup only while this attempt still owns its claim."""
        claim = self._claims[job_id]
        claim.finalizing = True
        key = in_progress_key_prefix + job_id
        result_ttl_ms = None if keep_result_forever else to_ms(result_timeout_s)
        # ARQ's unconditional deletion could otherwise erase a replacement worker's claim.
        async with asyncio.timeout(REDIS_SECONDS), self.pool.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(key)
                    if await pipe.get(key) != claim.token:
                        return
                    pipe.multi()
                    if keep_in_progress is None:
                        pipe.delete(key)
                    else:
                        pipe.pexpire(key, to_ms(keep_in_progress))
                    if finish and result_data:
                        pipe.set(
                            result_key_prefix + job_id,
                            result_data,
                            px=result_ttl_ms,
                        )
                    if finish:
                        pipe.delete(retry_key_prefix + job_id, job_key_prefix + job_id)
                        pipe.zrem(abort_jobs_ss, job_id)
                        pipe.zrem(self.queue_name, job_id)
                    if not finish and incr_score:
                        pipe.zincrby(self.queue_name, incr_score, job_id)
                    await pipe.execute()
                    return
                except WatchError:
                    continue

    async def finish_failed_job(self, job_id, result_data):
        """Keep failed-job results behind the same claim ownership check."""
        await self.finish_job(
            job_id,
            True,
            result_data if self.keep_result_forever or self.keep_result_s > 0 else None,
            self.keep_result_s,
            self.keep_result_forever,
            None,
            None,
        )


def create_fhir_catalog_worker(settings, **kwargs):
    """Construct the catalog worker with existing ARQ settings and overrides."""
    return FHIRCatalogWorker(**{**get_kwargs(settings), **kwargs})
