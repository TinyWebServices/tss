# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import hashlib, re, shutil
from pathlib import Path
from os.path import splitext
from os import getenv
from flask import Flask, abort, jsonify, request, send_file
from werkzeug.routing import Rule, Map, BaseConverter, ValidationError
from raven.contrib.flask import Sentry


KNOWN_MIME_TYPES = {
    "png": "image/png",
    "jpg": "image/jpeg",
    "gif": "image/gif",
    "mp3": "audio/mpeg",
    "mp4": "video/mp4",
    "txt": "text/plain",
    "html": "text/html",
    "css": "text/css",
    "js": "application/javascript",
    "json": "application/json",
    "xml": "text/xml",
}

DEFAULT_MIME_TYPE = "application/octet-stream"


app = Flask(__name__)
#app.url_map.converters['bucket_name'] = BucketNameConverter TODO
app.config["STORAGE_ROOT"] = getenv("STORAGE_ROOT", "/data/object-storage")
app.config["API_TOKEN"] = getenv("API_TOKEN", None)


sentry = Sentry(app)


def hash_object_name(object_name):
    return hashlib.sha1(object_name.encode()).hexdigest()

def mime_type_from_path(path):
    return KNOWN_MIME_TYPES.get(path.suffix[1:], DEFAULT_MIME_TYPE)

def mime_type_from_object_name(object_name):
    return mime_type_from_path(Path(object_name))

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

@app.route("/<bucket_name>", methods=["PUT"])
def put_bucket(bucket_name):
    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=True)
    return jsonify({})

@app.route("/<bucket_name>", methods=["DELETE"])
def delete_bucket(bucket_name):
    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=False)
    if not bucket_path.exists():
        abort(404)
    shutil.rmtree(str(bucket_path))
    return jsonify({})

#
# Objects
#

@app.route("/<bucket_name>/<path:object_name>", methods=["GET"])
def get_object(bucket_name, object_name):
    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=False)
    if not bucket_path.exists():
        abort(404)
    object_path = make_object_path(app.config["STORAGE_ROOT"], bucket_name, object_name, create=False)
    if not object_path.exists():
        abort(404)
    return send_file(str(object_path), mimetype=mime_type_from_object_name(object_name))

@app.route("/<bucket_name>/<path:object_name>", methods=["PUT"])
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
    return jsonify({})

@app.route("/<bucket_name>/<path:object_name>", methods=["DELETE"])
def delete_object(bucket_name, object_name):
    bucket_path = make_bucket_path(app.config["STORAGE_ROOT"], bucket_name, create=False)
    if not bucket_path.exists():
        abort(404)
    object_path = make_object_path(app.config["STORAGE_ROOT"], bucket_name, object_name, create=False)
    if not object_path.exists():
        abort(404)
    object_path.unlink()
    return jsonify({})
