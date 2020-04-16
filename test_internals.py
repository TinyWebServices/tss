# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pathlib
import flask, pytest

import tss

@pytest.fixture
def client(tmpdir_factory):
    app = tss.app
    app.config["STORAGE_ROOT"] = str(tmpdir_factory.mktemp("data"))
    app.config["SERVER_NAME"] = "localhost"
    with app.test_client() as c:
        with app.app_context():
            yield c

def test_put_bucket(client):
    r = client.put(flask.url_for('put_bucket', bucket_name="test"))
    assert r.status_code == 200

    bucket_path = pathlib.Path(tss.app.config["STORAGE_ROOT"], "buckets", "test")
    assert bucket_path.is_dir()

