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

def test_meta_data_headers(client):
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
        #assert r.headers["Content-Encoding"] == tss.DEFAULT_CONTENT_ENCODING
        #assert r.headers["X-TSS-Foo"] == "Bar"
        #assert r.headers["x-tss-something"] == "Something"

def dump_db():
    with tss.get_lmdb_env().begin() as tx:
        cursor = tx.cursor()
        cursor.first()
        for key, value in cursor:
            print(key, value)

def test_delete_meta_data(client, app):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test",
                   headers={"X-TSS-Foo": "Bar"})
    assert r.status_code == 200
    # Count number of values
    print("Before")
    dump_db()
    assert tss.get_lmdb_env().stat()["entries"] == 3
    # Delete the file
    r = client.delete(flask.url_for('delete_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    # Count number of values
    print("After")
    dump_db()
    assert tss.get_lmdb_env().stat()["entries"] == 0
