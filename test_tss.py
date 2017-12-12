#!/usr/bin/env python


import json, time
from pathlib import Path
from flask import url_for
import pytest

import tss

@pytest.fixture
def app(tmpdir):
    app = tss.app
    app.config["STORAGE_ROOT"] = str(tmpdir)
    return app

#
# Utility functions
#

def test_hash_object_name():
    assert (tss.hash_object_name("hello.txt")) == "3857b672471862eab426eba0622e44bd2cedbd5d"
    assert (tss.hash_object_name("foo/bar.txt")) == "52235c6fb17f9b5c405adf27d35f65ce1ff388cb"

def test_mime_type_from_path():
    for extension, mime_type in tss.KNOWN_MIME_TYPES.items():
        assert tss.mime_type_from_path(Path("test." + extension)) == mime_type
    assert tss.mime_type_from_path(Path("test.unknown")) == tss.DEFAULT_MIME_TYPE

def test_make_object_path(tmpdir):
    path = tss.make_object_path(tmpdir, "test-bucket", "hello.txt", create=False)
    assert path == tmpdir + "/test-bucket/38/57/b672471862eab426eba0622e44bd2cedbd5d"
    assert path.exists() == False
    assert path.parent.exists() == False

def test_make_object_path_create(tmpdir):
    path = tss.make_object_path(tmpdir, "test-bucket", "hello.txt", create=True)
    assert path == tmpdir + "/test-bucket/38/57/b672471862eab426eba0622e44bd2cedbd5d"
    assert path.exists() == False
    assert path.parent.exists() == True

def test_make_bucket_path(tmpdir):
    path = tss.make_bucket_path(tmpdir, "test-bucket", create=False)
    assert path == tmpdir + "/test-bucket"
    assert path.exists() == False

def test_make_bucket_path_create(tmpdir):
    path = tss.make_bucket_path(tmpdir, "test-bucket", create=True)
    assert path == tmpdir + "/test-bucket"
    assert path.exists() == True
    # Creating a bucket that already exists should not throw any exceptions
    path = tss.make_bucket_path(tmpdir, "test-bucket", create=True)
    assert path == tmpdir + "/test-bucket"
    assert path.exists() == True

#
# Buckets
#

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

#
# Objects
#

def test_get_object_404_bucket(client):
    # Non existing bucket
    res = client.get(url_for('get_object', bucket_name="doesnotexist", object_name="foo/bar/test.txt"))
    assert res.status_code == 404

def test_get_object_404_bucket(client):
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


# def test_delete_object(client):

#     # Non existing bucket
#     res = client.delete(url_for('delete_object', bucket_name="doesnotexist", object_name="1234567890abcdef123456.json"))
#     assert res.status_code == 404

#     # Existing bucket, invalid object name
#     res = client.get(url_for('delete_object', bucket_name="test", object_name="cheese.json"))
#     assert res.status_code == 404

#     # Existing bucket, non-existing object
#     res = client.get(url_for('delete_object', bucket_name="test", object_name="1234567890abcdef123456.json"))
#     assert res.status_code == 404

#     # Existing bucket, existing object
#     res = client.put(url_for('put_object', bucket_name="test", object_name="4234567890abcdef12345678.txt"), data="bacon")
#     assert res.status_code == 200
#     res = client.delete(url_for('delete_object', bucket_name="test", object_name="4234567890abcdef12345678.txt"))
#     assert res.status_code == 200
#     res = client.get(url_for('delete_object', bucket_name="test", object_name="4234567890abcdef12345678.txt"))
#     assert res.status_code == 404


# def test_put_object(client):

#     # Non-existing bucket, non-existing object
#     res = client.put(url_for('put_object', bucket_name="test", object_name="test.txt"), data="test")
#     assert res.status_code == 404

#     # res = client.get(url_for('get_object', bucket_name="test", object_name="888888888888888888888888.txt"))
#     # assert res.status_code == 200
#     # assert res.data == b"broccoli"

#     # Existing bucket, existing object (overwrite)
#     res = client.put(url_for('put_object', bucket_name="test_put_object", object_name="888888888888888888888888.txt"), data="cauliflower")
#     assert res.status_code == 200
#     res = client.get(url_for('get_object', bucket_name="test_put_object", object_name="888888888888888888888888.txt"))
#     assert res.status_code == 200
#     assert res.data == b"cauliflower"


# def test_parse_move_destination():

#     bucket_name, object_id = tss.parse_location_header("/test/aaaaaaaaaaaaaaaaaaaaaaaa.txt")
#     assert bucket_name == "test"
#     assert object_id == "aaaaaaaaaaaaaaaaaaaaaaaa.txt"

#     bucket_name, object_id = tss.parse_location_header("http://www.example.com/test/aaaaaaaaaaaaaaaaaaaaaaaa.txt")
#     assert bucket_name is None
#     assert object_id is None

#     bucket_name, object_id = tss.parse_location_header("/")
#     assert bucket_name is None
#     assert object_id is None

#     bucket_name, object_id = tss.parse_location_header("/test")
#     assert bucket_name is None
#     assert object_id is None

#     bucket_name, object_id = tss.parse_location_header("/test/")
#     assert bucket_name is None
#     assert object_id is None

#     bucket_name, object_id = tss.parse_location_header("/test/short.txt")
#     assert bucket_name is None
#     assert object_id is None


# def test_parse_overwrite_header():

#     assert tss.parse_overwrite_header(None) is True
#     assert tss.parse_overwrite_header("T") is True
#     assert tss.parse_overwrite_header("F") is False
#     assert tss.parse_overwrite_header("Cheese") is None


# def test_move_object(app, client):

#     # Put some test files
#     res = client.put(url_for('put_object', bucket_name="src", object_name="111111111111111111111111.txt"), data="one")
#     assert res.status_code == 200
#     res = client.put(url_for('put_object', bucket_name="src", object_name="222222222222222222222222.txt"), data="two")
#     assert res.status_code == 200
#     res = client.put(url_for('put_object', bucket_name="src", object_name="333333333333333333333333.txt"), data="three")
#     assert res.status_code == 200

#     #
#     # Destination does not exist yet
#     #

#     # The destination does not exist
#     res = client.get(url_for('get_object', bucket_name="dst", object_name="dddddddddddddddddddddddd.txt"))
#     assert res.status_code == 404

#     # Move it to another place
#     res = client.open(url_for('copy_move_object', bucket_name="src", object_name="111111111111111111111111.txt"),
#                       method="MOVE", headers={"Location": "/dst/dddddddddddddddddddddddd.txt"})
#     assert res.status_code == 201

#     # The original should be gone
#     res = client.get(url_for('get_object', bucket_name="src", object_name="111111111111111111111111.txt"))
#     assert res.status_code == 404

#     # The destination is now there
#     res = client.get(url_for('get_object', bucket_name="dst", object_name="dddddddddddddddddddddddd.txt"))
#     assert res.status_code == 200
#     assert res.data == b"one"

#     #
#     # Destination already exists
#     #

#     # Move it to another place
#     res = client.open(url_for('copy_move_object', bucket_name="src", object_name="222222222222222222222222.txt"),
#                       method="MOVE", headers={"Location": "/dst/dddddddddddddddddddddddd.txt"})
#     assert res.status_code == 204

#     # The original should be gone
#     res = client.get(url_for('get_object', bucket_name="src", object_name="2222222222222222222222222.txt"))
#     assert res.status_code == 404

#     # The destination is now there
#     res = client.get(url_for('get_object', bucket_name="dst", object_name="dddddddddddddddddddddddd.txt"))
#     assert res.status_code == 200
#     assert res.data == b"two"

#     #
#     # Destination already exists, Overwrite: F
#     #

#     # Move it to another place
#     res = client.open(url_for('copy_move_object', bucket_name="src", object_name="333333333333333333333333.txt"),
#                       method="MOVE", headers={"Location": "/dst/dddddddddddddddddddddddd.txt", "Overwrite": "F"})
#     assert res.status_code == 412

#     # The original should still be there
#     res = client.get(url_for('get_object', bucket_name="src", object_name="333333333333333333333333.txt"))
#     assert res.status_code == 200

#     # The destination has not been touched
#     res = client.get(url_for('get_object', bucket_name="dst", object_name="dddddddddddddddddddddddd.txt"))
#     assert res.status_code == 200
#     assert res.data == b"two"


# def test_mime_types(client):

#     def split_content_type(content_type):
#         return content_type.split(";", maxsplit=1)[0]

#     # Test if known file types come back as expected
#     for extension, mime_type in tss.KNOWN_MIME_TYPES.items():
#         object_name = "999999999999999999999999.%s" % extension
#         res = client.put(url_for('put_object', bucket_name="test_mime_types", object_name=object_name), data=mime_type)
#         assert res.status_code == 200
#         res = client.get(url_for('get_object', bucket_name="test_mime_types", object_name=object_name))
#         assert res.status_code == 200
#         assert split_content_type(res.headers['content-type']) == mime_type

#     # Test if unknown file types come back as application/octet-stream
#     res = client.put(url_for('put_object', bucket_name="test_mime_types", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.bin"), data="application/octet-stream")
#     assert res.status_code == 200
#     res = client.get(url_for('get_object', bucket_name="test_mime_types", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.bin"))
#     assert res.status_code == 200
#     assert split_content_type(res.headers['content-type']) == tss.DEFAULT_MIME_TYPE


# def test_get_buckets(app, client):
#     res = client.get(url_for("get_buckets"))
#     assert res.status_code == 200
#     assert type(res.json) == list
#     assert len(res.json) == 0

#     res = client.put(url_for('put_object', bucket_name="foo", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.txt"), data="test")
#     assert res.status_code == 200

#     res = client.get(url_for("get_buckets"))
#     assert res.status_code == 200
#     assert type(res.json) == list
#     assert len(res.json) == 1
#     assert "name" in res.json[0] and res.json[0]["name"] == "foo"
#     assert "created" in res.json[0]
#     assert "modified" in res.json[0]

#     res = client.put(url_for('put_object', bucket_name="bar", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.txt"), data="test")
#     assert res.status_code == 200

#     res = client.get(url_for("get_buckets"))
#     assert res.status_code == 200
#     assert type(res.json) == list
#     assert len(res.json) == 2


# def test_authentication(app, client):

#     app.config["API_TOKEN"] = "85cf8bccd8a5b42e0a2c2f64a3645ba8c0a7d625"

#     # Put something without authentication
#     res = client.put(url_for('put_object', bucket_name="test_authentication", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.txt"), data="test")
#     assert res.status_code == 401

#     # Put something with incorrect authentication
#     res = client.put(url_for('put_object', bucket_name="test_authentication", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.txt"), data="test")
#     assert res.status_code == 401

#     # Put something with correct authentication
#     res = client.put(url_for('put_object', bucket_name="test_authentication", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.txt"),
#                      data="test", headers={"Authorization": "token 85cf8bccd8a5b42e0a2c2f64a3645ba8c0a7d625"})
#     assert res.status_code == 200

