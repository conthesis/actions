from fastapi.testclient import TestClient

from actions.main import app

client = TestClient(app)


def test():
    req = {"kind": "identity", "properties": {"test": "test"}}
    res = client.post("/internal/compute", json=req)
    assert res.json() == {"test": "test"}
