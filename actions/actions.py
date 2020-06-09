from typing import Dict, Any


def identity(data: Dict[str, Any]):
    return data


async def add(a: Any, b: Any):
    return a + b
