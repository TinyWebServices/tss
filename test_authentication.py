# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import flask, pytest
import tss

TEST_API_TOKEN = "85cf8bccd8a5b42e0a2c2f64a3645ba8c0a7d625"

@pytest.fixture
def client(tmpdir_factory):
    app = tss.app
    app.config["STORAGE_ROOT"] = str(tmpdir_factory.mktemp("data"))
    app.config["API_TOKEN"] = TEST_API_TOKEN
    app.config["SERVER_NAME"] = "localhost"
    with app.test_client() as c:
        with app.app_context():
            yield c
    del app.config["API_TOKEN"]

def test_simple(client):
    res = client.get('/')
    assert res.status_code == 401

def test_authentication_401(client):
    # Put something without authentication
    res = client.put(flask.url_for('put_object', bucket_name="test_authentication", object_name="test.txt"),
                     data="test")
    assert res.status_code == 401
    # Put something with incorrect authentication
    res = client.put(flask.url_for('put_object', bucket_name="test_authentication", object_name="test.txt"),
                     data="test", headers={"Authorization": "token invalidtoken"})
    assert res.status_code == 401

def test_authentication_200(client):
    # Create a bucket
    r = client.put(flask.url_for('put_bucket', bucket_name="test_authentication"),
                   headers={"Authorization": "token " + TEST_API_TOKEN})
    assert r.status_code == 200
    # Put something with correct authentication
    res = client.put(flask.url_for('put_object', bucket_name="test_authentication", object_name="test.txt"),
                     data="test", headers={"Authorization": "token " + TEST_API_TOKEN})
    assert res.status_code == 200
