import msgpack
import time
import asyncio
import os
import aredis
import traceback
from .model import Action, ActionTrigger, ActionProperty
from transitions import Transition
from transitions.extensions.asyncio import AsyncMachine
from enum import Enum
from typing import Any, Optional
import logging

log = logging.getLogger("jobs")


JOB_LOCK_LEASE_TIMEOUT = 5

JOB_RUNNING_TIMEOUT = 30

def ts_now():
    return int(time.time())


class UnableToAcquireLockError(Exception):
    pass

class DataMissing(Exception):
    pass

class TriggerDataMissing(DataMissing):
    pass

class VariablesDataMissing(DataMissing):
    pass

class JobsManager:
    def __init__(self, svc):
        self.svc = svc
        redis = aredis.StrictRedis.from_url(os.environ["REDIS_URL"])
        self.storage = Storage(redis)
        self.run = True

    async def register(self, trigger):
        async with JidSession(self.svc, self.storage, trigger.jid) as j:
            await j.storage.set_trigger(trigger) # TODO: Ugly check this.
            time_remaining = await _in_x_seconds(3)
            await j.process(time_remaining)

    async def process(self, jid, src_state=None, blocking=True):
        async with JidSession(self.svc, self.storage, jid, blocking=blocking, src_state=src_state) as j:
            time_remaining = await _in_x_seconds(3)
            await j.process(time_remaining)

    async def resume(self, jid, data):
        async with JidSession(self.svc, self.storage, jid) as j:
            time_remaining = await _in_x_seconds(3)
            await j.resume_and_process(time_remaining, "success", data)

    async def setup(self):
        self.periodic = asyncio.create_task(self.periodic_check_loop())

    async def periodic_check_loop(self):
        log.info("Periodic check running")
        while True:
            try:
                await self.periodic_check()
            except Exception:
                traceback.print_exc()
            for i in range(5):
                await asyncio.sleep(1)
                if not self.run:
                    return

    async def stop():
        self.run = False
        await self.periodic

    async def periodic_check(self):
        for status in [Status.RUNNING, Status.PENDING, Status.RETRY]:
            jobs = await self.storage.random_sample(status)
            n_jobs = len(jobs)
            if n_jobs > 0:
                log.info(f"Found {n_jobs} jobs with state {status}")
            for jid in jobs:
                try:
                    await self.process(jid.decode("utf-8"), blocking=False, src_state=status.value.encode("utf-8"))
                except UnableToAcquireLockError:
                    pass
                except Exception:
                    log.error(f"Error handling jid={jid}")
                    traceback.print_exc()


class Status(Enum):
    PENDING = "PENDING"
    VARIABLES_LOADED = "VARIABLES_LOADED"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    RETRY = "RETRY"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    REVOKED = "REVOKED"


def make_status_machine(model, initial=None):
    # The states
    S = Status
    events = [
        { "trigger": "proceed", "source": [S.PENDING], "dest": S.VARIABLES_LOADED, "before": "load_data"},
        { "trigger": "proceed", "source": [S.VARIABLES_LOADED, S.RETRY], "dest": S.RUNNING, "after": "start_run"},
        { "trigger": "suspend", "source": [S.RUNNING], "dest": S.RUNNING },
        { "trigger": "succeeded", "source": [S.RUNNING], "dest": S.SUCCESS },
        { "trigger": "expired", "source": [S.PENDING, S.RETRY], "dest": S.FAILURE },
        { "trigger": "error", "source": [S.RUNNING], "dest": S.RETRY },
        { "trigger": "revoke", "source": [S.PENDING, S.VARIABLES_LOADED, S.RUNNING, S.RETRY], "dest": S.REVOKED},
    ]

    if initial is None:
        initial = S.PENDING

    return AsyncMachine(model, states=Status, transitions=events, initial=initial, ignore_invalid_triggers=True)

async def _in_x_seconds(secs):
    async def _inner(x):
        await asyncio.sleep(x)
    fut =  asyncio.create_task(_inner(secs))
    return lambda: not fut.done()

class JidSession:
    def __init__(self, service, storage, jid, blocking=True, src_state=None):
        self.service = service
        self.storage = storage
        self.jid = jid
        self.jid_storage = JidStorage(self.jid, self.storage, src_state=src_state)
        self.blocking = blocking

    async def __aenter__(self):
        locked = await (await self.jid_storage.lock()).acquire(blocking=self.blocking)

        if not locked:
            raise UnableToAcquireLockError()

        self.job = Job(self.jid, self.jid_storage, self.service, await self.jid_storage.get_state())
        return self.job

    async def __aexit__(self, exc_type, exc, tb):
        await self.jid_storage.set_state(self.job.state)
        await self.jid_storage.flush()
        await (await self.jid_storage.lock()).release()

STORAGE_EXPIRY = 6 * 60 * 60

class Storage:
    def __init__(self, redis):
        self.redis = redis

    def _key(self, jid):
        if isinstance(jid, bytes):
            jid = jid.decode("utf-8")
        return f"job-{jid}"

    def _set_key(self, state):
        return f"job-state-{state}"

    async def lock(self, jid):
        if isinstance(jid, bytes):
            jid = jid.decode("utf-8")
        return self.redis.lock(f"job-lock-{jid}", timeout=JOB_LOCK_LEASE_TIMEOUT)

    async def set(self, jid, params, src_state):
        if isinstance(jid, bytes):
            jid = jid.decode("utf-8")

        await self.redis.hmset(self._key(jid), params)
        await self.redis.expire(self._key(jid), STORAGE_EXPIRY)

        if "state" in params:
            dst = params["state"]
            if src_state is not None:
                src = src_state.decode("utf-8")
                if src != dst:
                    log.info(f"State for {jid} altered from {src} to {dst}")
                    await self.redis.srem(self._set_key(src), jid)
                    await self.redis.sadd(self._set_key(dst), jid)
            else:
                log.info(f"State for {jid} became {dst}")
                await self.redis.sadd(self._set_key(dst), jid)

    async def get(self, jid, key):
        return await self.redis.hget(self._key(jid), key)

    async def random_sample(self, state, n=15):
        state_name = state if isinstance(state, str) else state.value
        return await self.redis.srandmember(self._set_key(state_name), n)


class JidStorage:
    def __init__(self, jid, storage, src_state=None):
        self.jid = jid
        self.storage = storage
        self.dirty = set()
        self.cached = {}
        self.src_state = src_state
        self.flushing = None
        self._lock = None

    async def lock(self):
        if self._lock is None:
            self._lock = await self.storage.lock(self.jid)
        return self._lock

    async def flush(self):
        if self.flushing is not None:
            raise RuntimeError("May not flush while flush is in progress")

        flushed = { k: self.cached[k] for k in self.dirty }
        await self.storage.set(self.jid, flushed, self.src_state)
        self.cached = {}
        self.dirty = set()

    async def set(self, key: str, data: Any) -> None:
        self.dirty.add(key)
        self.cached[key] = data

    async def get(self, key: str) -> Any:
        if key not in self.cached:
            val = await self.storage.get(self.jid, key)
            self.cached[key] = val
            if self.src_state is None and key == "state":
                self.src_state = val
            return val
        return self.cached[key]

    async def get_trigger(self):
        data = await self.get("trigger")
        if data is None:
            return None
        try:
            return ActionTrigger.from_bytes(data)
        except:
            log.error(f"Trigger was invalid JSON {data}")
            return None


    async def set_trigger(self, data):
        return await self.set("trigger", data.to_bytes())

    async def get_action(self):
        return Action.from_bytes(await self.get("action"))

    async def set_action(self, data):
        return await self.set("action", data.to_bytes())

    async def get_variables(self):
        try:
            vs = msgpack.unpackb(await self.get("variables"))
            return [
                ActionProperty.parse_obj(x)
                for x in vs
            ]
        except:
            raise VariablesDataMissing()


    async def set_variables(self, data):
        return await self.set("variables", ActionProperty.many_to_bytes(data))

    async def set_timestamp(self, val):
        return await self.set("timestamp", str(val))

    async def get_timestamp(self):
        ts = await self.get("timestamp")
        if ts is None:
            return None
        return int(ts)

    async def get_state(self):
        state = await self.get("state")
        if state is None:
            return None
        return Status(state.decode("utf-8"))

    async def set_state(self, data):
        return await self.set("state", data.value)


class Job:
    storage: JidStorage
    def __init__(self, jid, storage, service, initial):
        self.jid = jid
        self.machine = make_status_machine(self, initial)
        self.storage = storage
        self.service = service

    async def load_data(self):
        trigger = await self.storage.get_trigger()
        if trigger is None:
            raise TriggerDataMissing()
        action = await self.service.get_action(trigger)
        await self.storage.set_action(action)
        if any(p is None for p in action.properties):
            log.error(f"Property in {action} was None")
        variables = await self.service.freeze_properties(action.properties, trigger.meta)
        await self.storage.set_variables(variables)


    async def start_run(self):
        action = await self.storage.get_action()
        variables = await self.storage.get_variables()
        resolved = await self.service.resolve_properties(variables)
        await self.service.perform_action_async(self.jid, action.kind, resolved)
        await self.storage.set_timestamp(ts_now())

    async def proceed_many(self, time_remaining):
        while time_remaining():
            if self.state == Status.RUNNING:
                return True
            if not await self.proceed():
                return True
        return False


    async def has_timed_out(self):
        ts = await self.storage.get_timestamp()
        if ts is None:
            await self.storage.set_timestamp(ts_now())
            return False

        now = ts_now()
        elapsed = now - ts
        return elapsed > JOB_RUNNING_TIMEOUT


    async def process(self, time_remaining):
        try:
            if not await self.proceed_many(time_remaining):
                return
        except DataMissing:
            log.error("Unable to fetch trigger data, revoking action")
            await self.revoke()
            return
        if self.state is Status.RUNNING:
            if await self.has_timed_out():
                await self.error()

        elif self.state is Status.RETRY:
            await self.expired()

    async def resume(self, time_remaining, result, data):
        if not time_remaining():
            return False
        if result == "suspend":
            return await self.suspend(data)
        elif result == "success":
            return await self.succeeded()
        elif result == "error":
            return await self.error(data)

    async def resume_and_process(self, time_remaining, result, data):
        if not await self.resume(time_remaining, result, data):
            return False
        return await self.process(time_remaining)
