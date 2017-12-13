# Tiny Storage Service (TSS)

*Stefan Arentz, December 2017*

[![Build Status](https://travis-ci.org/st3fan/tss-server.svg?branch=master)](https://travis-ci.org/st3fan/tss-server) [![codecov](https://codecov.io/gh/st3fan/tss-server/branch/master/graph/badge.svg)](https://codecov.io/gh/st3fan/tss-server)

This is a small REST API for storing and retrieving files. Very much like S3.

> Work in progress. Sort of works for what I use it for at home. BUt needs a good bunch of attention to make it a more general tool.

TODO Write proper documentation.

## To be sorted

Run locally for development:

```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
FLASK_APP=api.py FLASK_DEBUG=1 flask run
```

Create the container:

```
docker build -t tss-server:latest .
```

Run the container in the foreground:

```
docker run --rm -v /data/tss:/data/tss -e API_TOKEN=YourSecretToken -p 8080:8080 tss-server
```

Run the container in the background:

```
docker run -d -v /data/tss:/data/tss -e API_TOKEN=YourSecretToken -p 8080:8080 tss-server
```
