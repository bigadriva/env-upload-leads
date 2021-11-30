FROM python:3.10

ENV ELASTIC_URL "http://elastic:9200"

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .


# CMD [ "python", "src/main.py", "data/test.csv", "Valorem" ]
CMD [ "/bin/sh", "src/run.sh" ]
