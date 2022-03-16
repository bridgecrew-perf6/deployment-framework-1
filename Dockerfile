FROM python:3.7

LABEL maintainer="Eugene Alekseev <e.alekseev@s7.ru>"

WORKDIR /df

COPY . /df

RUN apt-get update && apt-get -y install sqlite3 && apt-get install -y --no-install-recommends apt-utils && pip3 install --trusted-host pypi.python.org -r requirements.txt

VOLUME ["/df/drivers", "/df/metadata", "/df/processors", "/df/models", "/df/sources", "/df/dags", "/df/scripts"]
ENV PYTHONPATH="$PYTHONPATH:/df"
ENV LAUNCHED_AS="DF"

EXPOSE 80
