# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pathlib
import flask, pytest

import tss
from test_tss import app

def test_put_bucket(client, tmpdir):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200

    bucket_path = pathlib.Path(tmpdir, "buckets", "test")
    assert bucket_path.is_dir()

