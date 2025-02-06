FROM public.ecr.aws/docker/library/python:3.13-slim-bullseye

WORKDIR /wheels
ADD ./requirements-dev.txt /wheels

WORKDIR /opt
RUN apt update && apt install -y gcc nginx libaio1 parallel && apt autopurge && python3 -m venv venv && . venv/bin/activate && pip install --no-compile --upgrade pip && \
	pip install --no-compile -r /wheels/requirements-dev.txt -f /wheels \
        && rm -rf /wheels \
        && rm -rf /root/.cache/pip/* && \
        find . -name *.pyc -execdir rm -f {} \;

ARG HLTHPRT_LOG_CFG=./logging.yaml
ARG HLTHPRT_RELEASE="dev"
ARG HLTHPRT_ENVIRONMENT=test

ARG HLTHPRT_DB_POOL_MAX_SIZE=10
ARG HLTHPRT_DB_POOL_MIN_SIZE=1

ARG HLTHPRT_DB_HOST=localhost
ARG HLPRT_DB_PORT=5432
ARG HLTHPRT_DB_DATABASE=healthporta
ARG HLTHPRT_DB_SCHEMA='mrf'
ARG HLTHPRT_DB_USER=dmytro
ARG HLTHPRT_DB_PASSWORD=''
ARG HLTHPRT_REDIS_ADDRESS=redis://localhost:6379

ARG HLTHPRT_SAVE_PER_PACK=100


ENV HLTHPRT_LOG_CFG=${HLTHPRT_LOG_CFG}
ENV HLTHPRT_RELEASE=${HLTHPRT_RELEASE}
ENV HLTHPRT_ENVIRONMENT=${HLTHPRT_ENVIRONMENT}
ENV HLTHPRT_DB_POOL_MIN_SIZE=${HLTHPRT_DB_POOL_MIN_SIZE}
ENV HLTHPRT_DB_POOL_MAX_SIZE=${HLTHPRT_DB_POOL_MAX_SIZE}

ENV HLTHPRT_DB_HOST=${HLTHPRT_DB_HOST}
ENV HLPRT_DB_PORT=${HLPRT_DB_PORT}
ENV HLTHPRT_DB_USER=${HLTHPRT_DB_USER}
ENV HLTHPRT_DB_PASSWORD=${HLTHPRT_DB_PASSWORD}
ENV HLTHPRT_DB_DATABASE=${HLTHPRT_DB_DATABASE}
ENV HLTHPRT_DB_SCHEMA=${HLTHPRT_DB_SCHEMA}

ENV HLTHPRT_REDIS_ADDRESS=${HLTHPRT_REDIS_ADDRESS}
ENV HLTHPRT_SAVE_PER_PACK=${HLTHPRT_SAVE_PER_PACK}

ADD service/nginx.conf /etc/nginx/nginx.conf
ADD service/start_api.sh /usr/local/bin/start_api.sh
ADD service/run_import.sh /usr/local/bin/run_import.sh
RUN chmod a+x /usr/local/bin/start_api.sh /usr/local/bin/run_import.sh

COPY api/ /opt/api/
COPY db/ /opt/db/
COPY data/ /opt/data/
COPY alembic/ /opt/alembic/
COPY process/ /opt/process/
COPY logging.yaml main.py alembic.ini /opt/

EXPOSE 8080
CMD ["/usr/local/bin/start_api.sh"]
