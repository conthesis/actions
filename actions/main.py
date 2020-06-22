from fastapi import FastAPI

import actions.api as api
import actions.hooks as hooks

app = FastAPI()

app.include_router(api.router)

# app.on_event("startup")(hooks.startup)
app.on_event("shutdown")(hooks.shutdown)
