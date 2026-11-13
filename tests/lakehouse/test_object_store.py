"""S3 operations Iceberg and Trino rely on, against the configured object store.

"S3-compatible" is a claim, not a guarantee. Iceberg's S3FileIO and Trino's native
S3 file system need: put and overwrite with read-after-write, multipart upload for
large files, ranged reads (Parquet footers and column chunks), head (file size),
paginated listing (orphan-file removal, purge), single and batch delete, and a
proper 404. This module checks exactly those. The catalog's own metadata
operations (commit, snapshot, purge) are proven by test_slice.py on the same store.

    FLEETFORGE_LAKEHOUSE=1 FLEETFORGE_LAKEHOUSE_S3_ENDPOINT=http://127.0.0.1:9900 \\
      FLEETFORGE_LAKEHOUSE_S3_ACCESS_KEY=... FLEETFORGE_LAKEHOUSE_S3_SECRET_KEY=... \\
      pytest tests/lakehouse/test_object_store.py
"""

from __future__ import annotations

import hashlib
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from fleetforge.lakehouse.jobs.common import object_store, warehouse_bucket
from fleetforge.lakehouse.settings import LakehouseSettings

pytestmark = [pytest.mark.lakehouse, pytest.mark.lakehouse_s3]


@pytest.fixture(scope="module")
def store():
    s = LakehouseSettings()
    fs = object_store(s)
    prefix = f"{warehouse_bucket(s)}/conformance-{uuid.uuid4().hex[:8]}"
    yield fs, prefix
    fs.delete_dir(prefix)


def _put(fs, path: str, data: bytes) -> None:
    with fs.open_output_stream(path) as out:
        out.write(data)


def _get(fs, path: str) -> bytes:
    with fs.open_input_stream(path) as f:
        return f.read()


def test_put_overwrite_and_read_after_write(store):
    fs, prefix = store
    _put(fs, f"{prefix}/a.json", b'{"v": 1}')
    assert _get(fs, f"{prefix}/a.json") == b'{"v": 1}'
    _put(fs, f"{prefix}/a.json", b'{"v": 2}')  # metadata.json is never overwritten, data may be
    assert _get(fs, f"{prefix}/a.json") == b'{"v": 2}'


def test_multipart_upload_round_trips_exactly(store):
    fs, prefix = store
    data = os.urandom(23 * 1024 * 1024)  # several parts at any common part size
    _put(fs, f"{prefix}/big.parquet", data)
    got = _get(fs, f"{prefix}/big.parquet")
    assert hashlib.sha256(got).digest() == hashlib.sha256(data).digest()


def test_ranged_reads_and_head(store):
    fs, prefix = store
    data = bytes(range(256)) * 4096
    _put(fs, f"{prefix}/r.bin", data)
    assert fs.get_file_info(f"{prefix}/r.bin").size == len(data)
    with fs.open_input_file(f"{prefix}/r.bin") as f:
        assert f.read_at(16, len(data) - 16) == data[-16:]  # a Parquet footer read
        assert f.read_at(100, 5000) == data[5000:5100]


def test_listing_paginates_beyond_one_thousand_keys(store):
    from pyarrow import fs as pafs

    fs, prefix = store
    with ThreadPoolExecutor(16) as pool:
        list(pool.map(lambda i: _put(fs, f"{prefix}/many/{i:05d}", b"x"), range(1050)))
    infos = fs.get_file_info(pafs.FileSelector(f"{prefix}/many", recursive=True))
    assert len([i for i in infos if i.type == pafs.FileType.File]) == 1050


def test_single_and_batch_delete_and_missing_object(store):
    from pyarrow import fs as pafs

    fs, prefix = store
    _put(fs, f"{prefix}/d/one", b"1")
    fs.delete_file(f"{prefix}/d/one")
    assert fs.get_file_info(f"{prefix}/d/one").type == pafs.FileType.NotFound
    with pytest.raises(FileNotFoundError):
        _get(fs, f"{prefix}/d/one")
    fs.delete_dir_contents(f"{prefix}/many")  # DeleteObjects, in batches
    remaining = fs.get_file_info(pafs.FileSelector(f"{prefix}/many", allow_not_found=True))
    assert remaining == []
