# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import hashlib, json, re, shutil
from pathlib import Path
from os.path import splitext
from os import getenv
from flask import Flask, abort, jsonify, request, send_file, g
from werkzeug.routing import Rule, Map, BaseConverter, ValidationError
from werkzeug.wsgi import wrap_file
from raven.contrib.flask import Sentry
import lmdb


DEFAULT_CONTENT_TYPE = "application/octet-stream"
DEFAULT_CONTENT_ENCODING = "identity"


class BucketNameConverter(BaseConverter):
    def __init__(self, url_map):
        super().__init__(url_map)
        self.regex = r"[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]+"
    def to_python(self, value):
        return value
    def to_url(self, value):
        return value


app = Flask(__name__)
app.url_map.converters['bucket_name'] = BucketNameConverter
app.config["STORAGE_ROOT"] = getenv("TSS_STORAGE_ROOT", "/data/tss")
app.config["API_TOKEN"] = getenv("TSS_API_TOKEN", None)


sentry = Sentry(app)


def get_lmdb_env():
    env = getattr(g, '_env', None)
    if env is None:
        g._env = lmdb.open(app.config["STORAGE_ROOT"] + '/metadata')
    return g._env


def hash_object_name(object_name):
    return hashlib.sha1(object_name.encode()).hexdigest()

def make_object_path(storage_root, bucket_name, object_name, create=False):
    object_hash = hash_object_name(object_name)
    path = Path(storage_root, "buckets", bucket_name, object_hash[0:2], object_hash[2:4], object_hash[4:])
    if create and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    return path

def make_bucket_path(storage_root, bucket_name, create=False):
    path = Path(storage_root, "buckets", bucket_name)
    if create and not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    return path

def key_prefix(bucket_name, object_name):
    return f"{bucket_name}:{object_name}:".encode()

def split_key(key):
    return key.decode().split(":", 3)
    #return t[0].decode(), t[1].decode(), t[2].decode()

def my_send_file(path, headers):
    return app.response_class(wrap_file(request.environ, path.open()), mimetype=headers.get("Content-Type", DEFAULT_CONTENT_TYPE),
                              headers=headers, direct_passthrough=True)


#
# Authentication
#

@app.before_request
def authenticate():
    api_token = app.config.get("API_TOKEN")
    if api_token:
        if request.headers.get("Authorization") != "token " + api_token:
            abort(401)

#
# Buckets
#

@app.route("/<bucket_name:bucket_name>", methods=["PUT"])
def put_bucket(bucket_name):
    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=True)
    return jsonify({})

@app.route("/<bucket_name:bucket_name>", methods=["DELETE"])
def delete_bucket(bucket_name):
    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=False)
    if not bucket_path.exists():
        abort(404)
    shutil.rmtree(str(bucket_path))
    return jsonify({})

#
# Objects
#

@app.route("/<bucket_name:bucket_name>/<path:object_name>", methods=["GET"])
def get_object(bucket_name, object_name):

    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=False)
    if not bucket_path.exists():
        abort(404)

    object_path = make_object_path(app.config["STORAGE_ROOT"], bucket_name, object_name, create=False)
    if not object_path.exists():
        abort(404)

    headers = {
        "Content-Type": DEFAULT_CONTENT_TYPE,
        "Content-Encoding": DEFAULT_CONTENT_ENCODING,
    }

    with get_lmdb_env().begin() as tx:
        cursor = tx.cursor()
        prefix = key_prefix(bucket_name, object_name)
        cursor.set_range(prefix)
        for key, value in cursor:
            if not key.startswith(prefix):
                break
            _, _, header_name = split_key(key)
            header_value = value.decode()
            headers[header_name] = header_value

    return my_send_file(object_path, headers)


@app.route("/<bucket_name:bucket_name>/<path:object_name>", methods=["PUT"])
def put_object(bucket_name, object_name):

    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=False)
    if not bucket_path.exists():
        abort(404)

    object_path = make_object_path(app.config["STORAGE_ROOT"], bucket_name, object_name, create=True)
    with object_path.open(mode="wb") as f:
        while True:
            chunk = request.stream.read(128 * 1024)
            if len(chunk) == 0:
                break
            f.write(chunk)

    meta_data = {
        "Content-Type": request.headers.get("Content-Type", DEFAULT_CONTENT_TYPE),
        "Content-Encoding": request.headers.get("Content-Encoding", DEFAULT_CONTENT_ENCODING),
    }

    if meta_data["Content-Type"] == "":
        meta_data["Content-Type"] = DEFAULT_CONTENT_TYPE

    for name, value in request.headers.items():
        if name.startswith("X-Tss-"):
            meta_data[name] = value

    with get_lmdb_env().begin(write=True) as tx:
        for k, v in meta_data.items():
            tx.put(f"{bucket_name}:{object_name}:{k}".encode(), v.encode())

    return jsonify({})


@app.route("/<bucket_name:bucket_name>/<path:object_name>", methods=["DELETE"])
def delete_object(bucket_name, object_name):
    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=False)
    if not bucket_path.exists():
        abort(404)

    object_path = make_object_path(app.config["STORAGE_ROOT"], bucket_name, object_name, create=False)
    if not object_path.exists():
        abort(404)

    with get_lmdb_env().begin(write=True) as tx:
        cursor = tx.cursor()
        prefix = key_prefix(bucket_name, object_name)
        cursor.set_range(prefix)
        while cursor.key().startswith(prefix):
            cursor.delete()

    object_path.unlink() # TODO Should this move into the transaction?

    return jsonify({})
