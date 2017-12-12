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
    path = Path(storage_root, bucket_name, object_hash[0:2], object_hash[2:4], object_hash[4:])
    if create and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    return path

def make_bucket_path(storage_root, bucket_name, create=False):
    path = Path(storage_root, bucket_name)
    if create and not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    return path


# @app.before_first_request
# def do_something_only_once():
#     storage_root = Path(app.config["STORAGE_ROOT"])
#     if not storage_root.is_dir():
#         app.logger.error("Storage root %s does not exist" % storage_root)


# @app.before_request
# def authenticate():
#     api_token = app.config.get("API_TOKEN")
#     if api_token:
#         if request.headers.get("Authorization") != "token " + api_token:
#             abort(401)

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


# def parse_location_header(location):
#     if location and type(location) == str:
#         m = re.match(r"/([A-Za-z0-9-_]+)/([0-9a-f]{24}\.[0-9a-z]+)",  location)
#         if m:
#             return m.group(1), m.group(2)
#     return None, None


# def parse_overwrite_header(overwrite):
#     values = {"T": True, "F": False, None: True}
#     return values.get(overwrite)


# @app.route("/<bucket_name>/<objectid:object_name>", methods=["COPY", "MOVE"])
# def copy_move_object(bucket_name, object_name):
#     path = Path(app.config["STORAGE_ROOT"], bucket_name, hash_object_name(object_name))
#     if not path.exists():
#         abort(404)

#     dst_bucket_name, dst_object_name = parse_location_header(request.headers.get("Location"))
#     if dst_bucket_name is None or dst_object_name is None:
#         abort(400, "Missing or invalid Location header")

#     overwrite = parse_overwrite_header(request.headers.get("Overwrite"))
#     if overwrite is None:
#         abort(400, "Invalid Overwrite header")

#     dst_path = Path(app.config["STORAGE_ROOT"], dst_bucket_name, hash_object_name(dst_object_name))
#     dst_exists = dst_path.exists()

#     if path == dst_path:
#         abort(403) # Forbidden

#     if not overwrite and dst_exists:
#         abort(412) # Precondition failed

#     ensure_object_directory(app.config["STORAGE_ROOT"], dst_bucket_name, dst_object_name)
#     if request.method == "MOVE":
#         path.rename(dst_path)
#     else:
#         shutil.copyfile(str(src_path), str(dst_path))

#     if dst_exists:
#         return ("OK", 204)
#     else:
#         return ("OK", 201)


# @app.route("/<bucket_name>", methods=["GET"])
# def get_bucket(bucket_name):
#     path = Path(app.config["STORAGE_ROOT"], bucket_name)
#     if not path.is_dir():
#         abort(404)
#     result = []
#     for child in path.glob("*/*/*"):
#         stat = child.stat()
#         result.append({"name": child.name,
#                        "type": mime_type_from_path(child),
#                        "size": stat.st_size,
#                        "modified": stat.st_mtime,
#                        "created": stat.st_ctime})
#     return jsonify(result)


# @app.route("/", methods=["GET"])
# def get_buckets():
#     path = Path(app.config["STORAGE_ROOT"])
#     if not path.is_dir():
#         abort(404)
#     result = []
#     for child in path.glob("*"):
#         stat = child.stat()
#         result.append({"name": child.name,
#                        "modified": stat.st_mtime,
#                        "created": stat.st_ctime})
#     return jsonify(result)
