# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json, time
from pathlib import Path
from flask import url_for
import pytest

import tss

def test_hash_object_name():
    assert (tss.hash_object_name("hello.txt")) == "3857b672471862eab426eba0622e44bd2cedbd5d"
    assert (tss.hash_object_name("foo/bar.txt")) == "52235c6fb17f9b5c405adf27d35f65ce1ff388cb"

def test_make_object_path(tmpdir):
    path = tss.make_object_path(tmpdir, "test-bucket", "hello.txt", create=False)
    assert path == tmpdir + "/buckets/test-bucket/38/57/b672471862eab426eba0622e44bd2cedbd5d"
    assert path.exists() == False
    assert path.parent.exists() == False

def test_make_object_path_create(tmpdir):
    path = tss.make_object_path(tmpdir, "test-bucket", "hello.txt", create=True)
    assert path == tmpdir + "/buckets/test-bucket/38/57/b672471862eab426eba0622e44bd2cedbd5d"
    assert path.exists() == False
    assert path.parent.exists() == True

def test_make_bucket_path(tmpdir):
    path = tss.make_bucket_path(tmpdir, "test-bucket", create=False)
    assert path == tmpdir + "/buckets/test-bucket"
    assert path.exists() == False

def test_make_bucket_path_create(tmpdir):
    path = tss.make_bucket_path(tmpdir, "test-bucket", create=True)
    assert path == tmpdir + "/buckets/test-bucket"
    assert path.exists() == True
    # Creating a bucket that already exists should not throw any exceptions
    path = tss.make_bucket_path(tmpdir, "test-bucket", create=True)
    assert path == tmpdir + "/buckets/test-bucket"
    assert path.exists() == True
