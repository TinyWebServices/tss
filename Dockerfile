FROM python:3.8.2

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

RUN mkdir /data/
VOLUME /data

EXPOSE 8080

CMD [ "gunicorn", "--workers=8", "--timeout=90", "--bind=0.0.0.0:8080", "--name=tss", "tss:app" ]
