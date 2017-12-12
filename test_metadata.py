# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import flask, pytest

import tss
from test_tss import app

def test_mime_types(client):

    def split_content_type(content_type):
        return content_type.split(";", maxsplit=1)[0]

    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test_mime_types"))
    assert r.status_code == 200

    # Test if known file types come back as expected
    for extension, mime_type in tss.KNOWN_MIME_TYPES.items():
        object_name = "test.%s" % extension
        res = client.put(flask.url_for('put_object', bucket_name="test_mime_types", object_name=object_name), data=mime_type)
        assert res.status_code == 200
        res = client.get(flask.url_for('get_object', bucket_name="test_mime_types", object_name=object_name))
        assert res.status_code == 200
        assert split_content_type(res.headers['content-type']) == mime_type

    # Test if unknown file types come back as application/octet-stream
    res = client.put(flask.url_for('put_object', bucket_name="test_mime_types", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.bin"), data="application/octet-stream")
    assert res.status_code == 200
    res = client.get(flask.url_for('get_object', bucket_name="test_mime_types", object_name="aaaaaaaaaaaaaaaaaaaaaaaa.bin"))
    assert res.status_code == 200
    assert split_content_type(res.headers['content-type']) == tss.DEFAULT_MIME_TYPE
