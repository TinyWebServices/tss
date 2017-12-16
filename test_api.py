# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import flask, pytest, requests

import tss
from test_tss import app

def test_put_bucket(client):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200

def test_get_bucket(client):
    # Fill up a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    for n in range(17):
        r = client.put(flask.url_for('put_object', bucket_name="test", object_name=f"test-{n}.txt"), data=f"This is test file #{n}")
        assert r.status_code == 200
    # Get the bucket contents
    r = client.get(flask.url_for('get_bucket', bucket_name="test"))
    assert r.status_code == 200
    assert type(r.json) == list
    assert len(r.json) == 17

def test_get_bucket_404(client):
    r = client.get(flask.url_for('get_bucket', bucket_name="doesnotexist"))
    assert r.status_code == 404

def parse_test_link_header(value):
    LINK_RE = re.compile("<http://localhost/test([^>]+)>; rel=next$")
    m = LINK_RE.match(value)
    if m:
        return m.group(1)

def test_get_bucket_paging(client, app):
    # Fill up a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    for n in range(234):
        r = client.put(flask.url_for('put_object', bucket_name="test", object_name="test-%.3d.txt" % n), data=f"This is test file #{n}")
        assert r.status_code == 200
    # Get the bucket contents
    r = client.get(flask.url_for('get_bucket', bucket_name="test"))
    assert r.status_code == 200
    assert type(r.json) == list
    assert len(r.json) == 100
    assert r.json[0]["Key"] == "test-000.txt"
    assert r.json[99]["Key"] == "test-099.txt"
    assert "Link" in r.headers
    next_link = parse_test_link_header(r.headers["Link"])
    assert next_link != None
    #
    r = client.get(next_link)
    assert r.status_code == 200
    assert type(r.json) == list
    assert len(r.json) == 100
    assert r.json[0]["Key"] == "test-100.txt"
    assert r.json[99]["Key"] == "test-199.txt"
    assert "Link" in r.headers
    next_link = parse_test_link_header(r.headers["Link"])
    assert next_link != None
    #
    r = client.get(next_link)
    assert r.status_code == 200
    assert type(r.json) == list
    assert len(r.json) == 34
    assert "Link" not in r.headers

def test_get_bucket_paging_one_page_minus_one(client, app):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    for n in range(99):
        r = client.put(flask.url_for('put_object', bucket_name="test", object_name=f"test-{n}.txt"), data=f"This is test file #{n}")
        assert r.status_code == 200
    r = client.get(flask.url_for('get_bucket', bucket_name="test"))
    assert r.status_code == 200
    assert type(r.json) == list
    assert len(r.json) == 99
    assert "Link" not in r.headers

def test_get_bucket_paging_one_page(client, app):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    for n in range(100):
        r = client.put(flask.url_for('put_object', bucket_name="test", object_name=f"test-{n}.txt"), data=f"This is test file #{n}")
        assert r.status_code == 200
    r = client.get(flask.url_for('get_bucket', bucket_name="test"))
    assert r.status_code == 200
    assert type(r.json) == list
    assert len(r.json) == 100
    assert "Link" not in r.headers

def test_get_bucket_paging_one_page_plus_one(client, app):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    for n in range(101):
        r = client.put(flask.url_for('put_object', bucket_name="test", object_name=f"test-{n}.txt"), data=f"This is test file #{n}")
        assert r.status_code == 200
    r = client.get(flask.url_for('get_bucket', bucket_name="test"))
    assert r.status_code == 200
    assert type(r.json) == list
    assert len(r.json) == 100
    assert "Link" in r.headers

def test_delete_bucket_404(client):
    # Delete a bucket that does not exist
    r = client.delete(flask.url_for('delete_bucket', bucket_name="test"))
    assert r.status_code == 404

def test_delete_bucket_200(client):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    r = client.delete(flask.url_for('delete_bucket', bucket_name="test"))
    assert r.status_code == 200

def test_get_object_404_on_bucket(client):
    # Non existing bucket
    res = client.get(flask.url_for('get_object', bucket_name="doesnotexist", object_name="foo/bar/test.txt"))
    assert res.status_code == 404

def test_get_object_404_on_object(client):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    res = client.get(flask.url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
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
