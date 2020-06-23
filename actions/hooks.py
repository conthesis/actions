from fastapi.dependencies.utils import solve_dependencies, get_dependant
import actions.deps as deps



async def startup():
    dep = get_dependant(path="", call=deps.worker)
    await solve_dependencies(dependant=dep, request=None)


async def shutdown():
    await (await deps.http_client()).aclose()
