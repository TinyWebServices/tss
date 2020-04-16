# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import re
import flask
import pytest
import tss


@pytest.fixture
def client(tmpdir_factory):
    app = tss.app
    app.config["STORAGE_ROOT"] = str(tmpdir_factory.mktemp("data"))
    app.config["SERVER_NAME"] = "localhost"
    with app.test_client() as c:
        with app.app_context():
            yield c

def test_get_object_404_on_bucket(client):
    # Non existing bucket
    res = client.get(flask.url_for('get_object', bucket_name="doesnotexist", object_name="foo/bar/test.txt"))
    assert res.status_code == 404

def test_get_object_404_on_object(client):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    res = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert res.status_code == 404

def test_head_object_404_on_object(client):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    res = client.head(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert res.status_code == 404

def test_get_object_200(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 200
    # Get the objct
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.data == b"test"

def test_head_object_200(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"),
                   data="test", headers={"Content-Type": "text/plain", "X-TSS-Foo": "foo", "X-TSS-Bar": "bar"})
    assert r.status_code == 200
    # Get the objct
    r = client.head(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "text/plain"
    assert r.headers["Content-Encoding"] == "identity"
    assert r.headers["Content-Length"] == "4"
    assert r.headers["X-TSS-Foo"] == "foo"
    assert r.headers["X-TSS-Bar"] == "bar"

def test_put_object_404(client):
    # Store an object in a bucket that does not exist
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 404

def test_put_object_200(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object in a bucket that does not exist
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 200
    # Get the object
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.data == b"test"

def test_delete_object_404_bucket(client):
    # Delete the object
    r = client.delete(flask.url_for('delete_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 404

def test_delete_object_404_object(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Delete the object
    r = client.delete(flask.url_for('delete_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 404

def test_delete_object_200(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 200
    # Get the object
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    # Delete the object
    r = client.delete(flask.url_for('delete_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    # Get the object
    r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 404

def test_put_object_sizes_text_plain(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Put objects of different sizes
    for size in (0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384):
        # Put
        data = ("X" * size).encode()
        r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test%d.txt" % size), data=data, headers={"Content-Type": "text/plain"})
        assert r.status_code == 200
        # Get
        r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test%d.txt" % size))
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "text/plain; charset=utf-8"
        assert r.data == data

def test_put_object_sizes_image_png(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Put objects of different sizes
    for size in (0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384):
        # Put
        data = ("X" * size).encode()
        r = client.put(flask.url_for('put_object', bucket_name="test", object_name="foo/bar/test%d.txt" % size), data=data, headers={"Content-Type": "image/png"})
        assert r.status_code == 200
        # Get
        r = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test%d.txt" % size))
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "image/png"
        assert r.data == data
