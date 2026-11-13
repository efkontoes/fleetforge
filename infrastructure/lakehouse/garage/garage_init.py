"""Idempotent first-start setup of the single Garage node, via its admin API (v2).

Garage needs three things before S3 works: a cluster layout (which node stores
what), an access key, and a bucket the key may use. The Garage image has no shell,
so this runs in a separate one-shot container. Every step checks first, so it is
safe to run on every `up`.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

ADMIN = os.environ.get("GARAGE_ADMIN_URL", "http://garage:3903")
TOKEN = os.environ["GARAGE_ADMIN_TOKEN"]
KEY_ID = os.environ["LAKEHOUSE_S3_ACCESS_KEY"]
SECRET = os.environ["LAKEHOUSE_S3_SECRET_KEY"]
BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
CAPACITY = 10 * 1024**3  # bytes the node offers; a layout needs a number


def call(method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{ADMIN}/v2/{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read()
    return json.loads(raw) if raw else {}


def exists(path: str) -> dict | None:
    try:
        return call("GET", path)
    except urllib.error.HTTPError as exc:
        if exc.code in (400, 404):
            return None
        raise


def main() -> int:
    for _ in range(60):
        try:
            status = call("GET", "GetClusterStatus")
            break
        except (urllib.error.URLError, ConnectionError):
            time.sleep(1)
    else:
        print("garage admin API not reachable", file=sys.stderr)
        return 1
    if status["layoutVersion"] == 0:
        node = status["nodes"][0]["id"]
        call(
            "POST",
            "UpdateClusterLayout",
            {"roles": [{"id": node, "zone": "dc1", "capacity": CAPACITY, "tags": []}]},
        )
        call("POST", "ApplyClusterLayout", {"version": 1})
        print(f"layout v1: node {node[:16]}")
    for _ in range(60):
        if call("GET", "GetClusterHealth")["status"] == "healthy":
            break
        time.sleep(1)
    if exists(f"GetKeyInfo?id={KEY_ID}") is None:
        call(
            "POST",
            "ImportKey",
            {"accessKeyId": KEY_ID, "secretAccessKey": SECRET, "name": "lakehouse"},
        )
        print("key imported")
    bucket = exists(f"GetBucketInfo?globalAlias={BUCKET}")
    if bucket is None:
        bucket = call("POST", "CreateBucket", {"globalAlias": BUCKET})
        print(f"bucket {BUCKET} created")
    call(
        "POST",
        "AllowBucketKey",
        {
            "bucketId": bucket["id"],
            "accessKeyId": KEY_ID,
            "permissions": {"read": True, "write": True, "owner": True},
        },
    )
    print("garage ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
