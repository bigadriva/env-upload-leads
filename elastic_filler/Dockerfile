FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

ENV LOCAL_ELASTIC_URL 'http://elastic:9200'
ENV REMOTE_ELASTIC_URL 'http://elastic.datadriva.com'
ENV REMOTE_ELASTIC_USER 'elastic'
ENV REMOTE_ELASTIC_PASSWORD 'BvfrG2NHXFa9qm'

ENV EB_INDEX 'empresasdobrasilv13'

CMD [ "python", "src/main.py" ]
