"""Milvus data-plane smoke test against an Instance's connection Secret.

MODE=full (default) writes, searches and drops a collection.
MODE=write leaves the collection behind; MODE=verify checks it is still there
and searchable, then drops it. Run write and verify around an upgrade or
restart to prove data survives it. MODE=rejected passes only when the
connection fails, e.g. a plain-text client against a TLS instance.

MILVUS_CA, when set, is the CA that verifies a TLS instance (the connection
Secret's ca.crt). MILVUS_REST_URL, when set, is checked over REST as well.
Exits non-zero on any failure.
"""

import json
import os
import random
import ssl
import sys
import urllib.request

from pymilvus import DataType, MilvusClient
from pymilvus.client.types import LoadState

COLLECTION = "openeverest_smoke"
DIM = 8
ROWS = 200
PROBE_ID = 42


def vectors() -> list[dict]:
    rng = random.Random(7)
    return [{"id": i, "vector": [rng.random() for _ in range(DIM)]} for i in range(ROWS)]


def write(client: MilvusClient) -> None:
    if client.has_collection(COLLECTION):
        client.drop_collection(COLLECTION)
    schema = client.create_schema(auto_id=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=DIM)
    index = client.prepare_index_params()
    index.add_index(field_name="vector", index_type="AUTOINDEX", metric_type="L2")
    client.create_collection(COLLECTION, schema=schema, index_params=index)
    client.insert(COLLECTION, vectors())
    client.flush(COLLECTION)
    # Loading with several replicas pins one query and one streaming node each.
    if replicas := int(os.environ.get("REPLICAS", "0")):
        client.load_collection(COLLECTION, replica_number=replicas)


def verify(client: MilvusClient) -> bool:
    if client.get_load_state(COLLECTION)["state"] != LoadState.Loaded:
        client.load_collection(COLLECTION)
    hits = client.search(COLLECTION, data=[vectors()[PROBE_ID]["vector"]], limit=3, output_fields=["id"])
    top = hits[0][0]["id"]
    count = client.query(COLLECTION, filter="", output_fields=["count(*)"])[0]["count(*)"]
    print(f"top hit: {top}, row count: {count}")
    return top == PROBE_ID and count == ROWS


def ca_path() -> str | None:
    ca = os.environ.get("MILVUS_CA", "")
    if not ca:
        return None
    path = "/tmp/milvus-ca.crt"
    with open(path, "w", encoding="utf-8") as f:
        f.write(ca)
    return path


def connect(tls: bool = True) -> MilvusClient:
    kwargs = {}
    if tls and (ca := ca_path()):
        kwargs["server_pem_path"] = ca
    return MilvusClient(uri=os.environ["MILVUS_URI"], token=os.environ["MILVUS_TOKEN"], timeout=20, **kwargs)


def check_rest(url: str) -> None:
    context = ssl.create_default_context(cafile=ca_path())
    request = urllib.request.Request(
        url + "/v2/vectordb/collections/list",
        data=b"{}",
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + os.environ["MILVUS_TOKEN"]},
    )
    with urllib.request.urlopen(request, context=context, timeout=20) as response:
        body = json.load(response)
    if body.get("code") != 0:
        raise RuntimeError(f"REST call failed: {body}")
    print("rest: ok")


def main() -> int:
    mode = os.environ.get("MODE", "full")
    if mode == "rejected":
        try:
            connect(tls=False).get_server_version()
        except Exception as e:  # noqa: BLE001 - any failure to connect is the expected outcome
            print(f"rejected as expected: {type(e).__name__}")
            print("PASS")
            return 0
        print("FAIL: the connection was accepted")
        return 1

    client = connect()
    print("server version:", client.get_server_version())
    if url := os.environ.get("MILVUS_REST_URL"):
        check_rest(url)

    if mode in ("full", "write"):
        write(client)
    ok = True
    if mode in ("full", "verify"):
        ok = verify(client)
        client.drop_collection(COLLECTION)

    print("PASS" if ok else f"FAIL: expected top hit {PROBE_ID} and {ROWS} rows")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
