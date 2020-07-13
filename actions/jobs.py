from transitions import AsyncMachine, Transition
from enum import Enum
from typing import Any, Optional

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
        { "trigger": "proceed", "source": S.PENDING, "dest": S.VARIABLES_LOADED, "before": "load_data"},
        { "trigger": "proceed", "source": [S.VARIABLES_LOADED, S.RETRY], "dest": S.RUNNING, "after": "start_run"},
        ("suspend", S.RUNNING, S.RUNNING),
        ("expired", [S.PENDING, S.RETRY], S.FAILURE),
        ("succeeded", S.RUNNING, S.SUCCESS),
        ("error", S.RUNNING, S.RETRY),
        ("revoke", [S.PENDING, S.TRIGGER_LOADED, S.VARIABLES_LOADED, S.RUNNING, S.RETRY], S.REVOKED),
    ]

    if initial is None:
        initial = S.PENDING

    return AsyncMachine(model, states=Status, transitions=transitions, initial=initial, ignore_invalid_triggers=True)

class JidSession:
    def __init__(self, service, storage, jid):
        self.service = service
        self.storage = storage
        self.jid = jid
        self.jid_storage = JidStorage(self.storage, self.jid)

    async def __aenter__(self):
        await self.jid_storage.lock.acquire(blocking=True)
        return Job(self.jid_storage, self.service, await self.jid_storage.get_state())

    async def __aexit__(self, exc_type, exc, tb):
        await self.jid_storage.flush()
        await self.jid_storage.lock.release()


class Storage:
    def __init__(self, redis):
        self.redis = redis

    def _key(self, jid, key):
        return f"job-{jid}-{key}"

    async def lock(self, jid):
        return self.redis.lock(f"job-lock-{jid}")

    async def set(self, jid, params):
        await self.redis.mset({ self._key(jid, k): v for (k, v) in params.items() })

    async def get(self, jid, key):
        return await self.redis.get(self._key(jid, key))



class JidStorage:
    def __init__(self, jid, storage):
        self.jid = jid
        self.storage = storage
        self.dirty = set()
        self.cached = {}
        self.flushing = None
        self.lock = self.storage.lock(self.jid)

    async def flush():
        if self.flushing is not None:
            raise RuntimeError("May not flush while flush is in progress")

        flushed = { k: self.cached[k] for k in self.dirty }
        await self.storage.set(self.jid, flushed)
        self.cached = {}
        self.dirty = set()

    def set(self, key: str, data: Any) -> None:
        self.dirty.add(data)
        self.cached[key] = data

    async def get(self, key: str) -> Any:
        if key not in self.cached:
            self.cached[key] = await self.storage.get(self, self.jid, key)
        return self.cached[key]

    async def get_trigger(self):
        return self.get("trigger")

    async def set_trigger(self, data):
        return self.set("trigger", data)

    async def get_action(self):
        return self.get("action")

    async def set_action(self, data):
        return self.set("action", data)

    async def get_variables(self):
        return self.get("variables")

    async def set_variables(self, data):
        return self.set("variables", data)

    async def get_state(self):
        return self.get("state")

    async def set_state(self, data):
        return self.set("state", data)


class Job:
    storage: JidStorage
    def __init__(self, storage, service, initial):
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
        await self.service.perform_action(action.kind, resovled)

    async def proceed_many(self, time_remaining):
        while time_remaining():
            if not await self.proceed():
                return True
        return False

    async def process(self, time_remaining):
        if not await self.proceed_many(time_remaining):
            return
        if self.state is Status.RUNNING:
            if self.has_timed_out():
                await self.error()

        elif self.state is Status.RETRY:
            await self.expired()

    async def resume(self, time_remaining, result, data):
        if not time_remaining():
            return False
        if result == "suspend":
            return await self.suspend(data)
        elif result == "success":
            return await self.succeeded(data)
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


class JobsManager:
    def __init__(self, svc):
        self.svc = svc
        self.storage = Storage()

    async def register(self, jid, trigger):
        # TODO: Add to storage here
        async with JidSession(self.svc, self.storage, jid) as j:
            await j.process()

    async def resume(self, result, data):
        async with JidSession(self.svc, self.storage, jid) as j:
            await j.resume_and_process()

    async def timeout(self):
        async with JidSession(self.svc, self.storage, jid) as j:
            await j.timeout_and_process()
