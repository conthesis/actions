import asyncio
import os
import aredis
import orjson
import traceback
from .model import Action, ActionTrigger
from transitions import Transition
from transitions.extensions.asyncio import AsyncMachine
from enum import Enum
from typing import Any, Optional

JOB_LOCK_LEASE_TIMEOUT = 5

class UnableToAcquireLockError(Exception):
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

    async def resume(self, jid, data):
        async with JidSession(self.svc, self.storage, jid) as j:
            time_remaining = await _in_x_seconds(3)
            await j.resume_and_process(time_remaining, "success", data)

    async def timeout(self, jid, blocking):
        async with JidSession(self.svc, self.storage, jid, blocking=blocking) as j:
            time_remaining = await _in_x_seconds(3)
            await j.timeout_and_process(time_remaining)


    async def setup(self):
        self.periodic = asyncio.create_task(self.periodic_check_loop())

    async def periodic_check_loop(self):
        print("Periodic check running")
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
        running = await self.storage.random_sample(Status.RUNNING)

        n_running = len(running)
        if n_running > 0:
            print(f"Found {n_running} jobs with state {Status.RUNNING}")
        for jid in running:
            try:
                await self.timeout(jid, False)
            except UnableToAcquireLockError:
                pass
            except Exception:
                print(f"Error handling jid={jid}")
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
    def __init__(self, service, storage, jid, blocking=True):
        self.service = service
        self.storage = storage
        self.jid = jid
        self.jid_storage = JidStorage(self.jid, self.storage)
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


class Storage:
    def __init__(self, redis):
        self.redis = redis

    def _key(self, jid, key):
        return f"job-{jid}-{key}"

    def _set_key(self, state):
        return f"job-state-{state}"

    async def lock(self, jid):
        return self.redis.lock(f"job-lock-{jid}", timeout=JOB_LOCK_LEASE_TIMEOUT)

    async def set(self, jid, params, src_state):
        await self.redis.mset({ self._key(jid, k): v for (k, v) in params.items() })
        if "state" in params:
            await self.redis.srem(self._set_key(src_state), jid)
            await self.redis.sadd(self._set_key(params["state"]), jid)

    async def get(self, jid, key):
        return await self.redis.get(self._key(jid, key))

    async def random_sample(self, state, n=15):
        state_name = state if isinstance(state, str) else state.value
        return await self.redis.srandmember(state_name, n)


class JidStorage:
    def __init__(self, jid, storage):
        self.jid = jid
        self.storage = storage
        self.dirty = set()
        self.cached = {}
        self.src_state = None
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
            if key == "state":
                self.src_state = val
            return val
        return self.cached[key]

    async def get_trigger(self):
        data = await self.get("trigger")
        if data is None:
            return None
        return ActionTrigger.from_bytes(data)

    async def set_trigger(self, data):
        return await self.set("trigger", data.to_bytes())

    async def get_action(self):
        return Action.from_bytes(await self.get("action"))

    async def set_action(self, data):
        return await self.set("action", data.to_bytes())

    async def get_variables(self):
        return orjson.loads(await self.get("variables"))

    async def set_variables(self, data):
        return await self.set("variables", orjson.dumps(data))

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
        action = await self.service.get_action(trigger)
        await self.storage.set_action(action)
        variables = await self.service.resolve_properties(action.properties, trigger.meta)
        await self.storage.set_variables(variables)


    async def start_run(self):
        action = await self.storage.get_action()
        variables = await self.storage.get_variables()
        await self.service.perform_action_async(self.jid, action.kind, variables)

    async def proceed_many(self, time_remaining):
        while time_remaining():
            if not await self.proceed():
                return True
        return False


    async def has_timed_out(self):
        return False

    async def process(self, time_remaining):
        if not await self.proceed_many(time_remaining):
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

    async def timeout_and_process(self, time_remaining):
        if not await self.error():
            return
        return await self.process(time_remaining)
