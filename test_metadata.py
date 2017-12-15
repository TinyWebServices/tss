# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import flask, pytest

import tss
from test_tss import app

def test_split_key():
    bucket_name, object_name, value = tss.split_key(b'test:foo/bar/test.txt:Content-Encoding')
    assert bucket_name == "test"
    assert object_name == "foo/bar/test.txt"
    assert value == "Content-Encoding"

def test_content_type(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test", headers={"Content-Type":"application/foobar"})
    assert r.status_code == 200
    # Get the objct
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "application/foobar"

def test_content_type_default(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 200
    # Get the objct
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.headers["Content-Type"] == tss.DEFAULT_CONTENT_TYPE

def test_content_encoding(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test",
                   headers={"Content-Encoding": "gzip"})
    assert r.status_code == 200
    # Get the objct
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.headers["Content-Encoding"] == "gzip"

def test_content_encoding_default(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 200
    # Get the objct
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.headers["Content-Encoding"] == tss.DEFAULT_CONTENT_ENCODING

def test_meta_data_headers(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test",
                   headers={"X-TSS-Something": "Something", "X-TSS-Foo": "Bar"})
    assert r.status_code == 200
    # Get the objct
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.headers["Content-Encoding"] == tss.DEFAULT_CONTENT_ENCODING
    assert r.headers["X-TSS-Foo"] == "Bar"
    assert r.headers["x-tss-something"] == "Something"

def test_meta_data_headers2(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store objects
    for n in range(10):
        r = client.put(flask.url_for('put_object', bucket_name="test", object_name=str(n)), data="test", headers={"X-TSS-N": str(n)})
        assert r.status_code == 200
    # Get the objects
    for n in range(10):
        r = client.get(flask.url_for('get_object', bucket_name="test", object_name=str(n)))
        assert r.status_code == 200
        assert r.headers["Content-Encoding"] == tss.DEFAULT_CONTENT_ENCODING
        assert r.headers["X-TSS-N"] == str(n)

# def dump_db():
#     with tss.get_lmdb_env().begin() as tx:
#         cursor = tx.cursor()
#         cursor.first()
#         for key, value in cursor:
#             print(key, value)

def test_delete_meta_data(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test",
                   headers={"X-TSS-Foo": "Bar"})
    assert r.status_code == 200
    # Count number of values
    assert tss.get_lmdb_env().stat()["entries"] == 4 # (Content-Type, Content-Encoding, Last-Modified + X-TSS-Foo)
    # Delete the file
    r = client.delete(flask.url_for('delete_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    # Count number of values
    assert tss.get_lmdb_env().stat()["entries"] == 0

def test_last_modified(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="test.txt"), data="test")
    assert r.status_code == 200
    # Get the objct
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="test.txt"))
    assert r.status_code == 200
    assert "Last-Modified" in r.headers

def test_content_length(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="test.txt"), data="test")
    assert r.status_code == 200
    # Get the objct
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="test.txt"))
    assert r.status_code == 200
    assert "Content-Length" in r.headers
    assert r.headers["Content-Length"] == "4"

def test_update_object(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="test.txt"), data="test", headers={"X-TSS-A": "Aa", "X-TSS-X": "X"})
    assert r.status_code == 200
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="test.txt"))
    assert r.status_code == 200
    assert "X-TSS-A" in r.headers
    assert r.headers["X-TSS-A"] == "Aa"
    assert "X-TSS-X" in r.headers
    assert r.headers["X-TSS-X"] == "X"
    assert r.headers["Content-Length"] == "4"
    # Update the object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="test.txt"), data="newtest", headers={"X-TSS-B": "Bbbb", "X-TSS-X": "XxX"})
    assert r.status_code == 200
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="test.txt"))
    assert r.status_code == 200
    assert "X-TSS-A" not in r.headers
    assert "X-TSS-B" in r.headers
    assert r.headers["X-TSS-B"] == "Bbbb"
    assert "X-TSS-X" in r.headers
    assert r.headers["X-TSS-X"] == "XxX"
    assert r.headers["Content-Length"] == "7"
