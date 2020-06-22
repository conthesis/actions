from typing import Any, Dict


def identity(data: Dict[str, Any]):
    return data


async def add(a: Any, b: Any):
    return a + b
