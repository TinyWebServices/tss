# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flask import url_for
import pytest

import tss
from test_tss import app

def test_put_bucket(client):
    r = client.put(url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200

def test_get_bucket(client):
    pass # TODO

def test_delete_bucket_404(client):
    # Delete a bucket that does not exist
    r = client.delete(url_for('delete_bucket', bucket_name="test"))
    assert r.status_code == 404

def test_delete_bucket_200(client):
    r = client.put(url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    r = client.delete(url_for('delete_bucket', bucket_name="test"))
    assert r.status_code == 200

def test_get_object_404_on_bucket(client):
    # Non existing bucket
    res = client.get(url_for('get_object', bucket_name="doesnotexist", object_name="foo/bar/test.txt"))
    assert res.status_code == 404

def test_get_object_404_on_object(client):
    r = client.put(url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    res = client.get(url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert res.status_code == 404

def test_get_object_200(client):
    # Create a bucket
    r = client.put(url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 200
    # Get the objct
    r = client.get(url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.data == b"test"

def test_put_object_404(client):
    # Store an object in a bucket that does not exist
    r = client.put(url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 404

def test_put_object_200(client):
    # Create a bucket
    r = client.put(url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object in a bucket that does not exist
    r = client.put(url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 200
    # Get the object
    r = client.get(url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    assert r.data == b"test"

def test_delete_object_404_bucket(client):
    # Delete the object
    r = client.delete(url_for('delete_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 404

def test_delete_object_404_object(client):
    # Create a bucket
    r = client.put(url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Delete the object
    r = client.delete(url_for('delete_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 404

def test_delete_object_200(client):
    # Create a bucket
    r = client.put(url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200
    # Store an object
    r = client.put(url_for('put_object', bucket_name="test", object_name="foo/bar/test.txt"), data="test")
    assert r.status_code == 200
    # Get the object
    r = client.get(url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    # Delete the object
    r = client.delete(url_for('delete_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 200
    # Get the object
    r = client.get(url_for('get_object', bucket_name="test", object_name="foo/bar/test.txt"))
    assert r.status_code == 404
