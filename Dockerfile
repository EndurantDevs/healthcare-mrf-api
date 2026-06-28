FROM rust:1-slim-trixie AS ptg2-scanner-builder

ARG TARGETARCH
ARG PTG2_SCANNER_RUSTFLAGS_AMD64="-C target-cpu=x86-64-v3"

WORKDIR /build
COPY support/ptg2_scanner/ /build/support/ptg2_scanner/
COPY process/ext/address_pub28.py /build/process/ext/address_pub28.py
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && python3 -m pip install --break-system-packages --no-cache-dir "maturin>=1.14,<2" \
    && rm -rf /var/lib/apt/lists/*
RUN if [ "${TARGETARCH:-amd64}" = "amd64" ]; then \
        RUSTFLAGS="${PTG2_SCANNER_RUSTFLAGS_AMD64}" cargo build --release --manifest-path /build/support/ptg2_scanner/Cargo.toml; \
    else \
        cargo build --release --manifest-path /build/support/ptg2_scanner/Cargo.toml; \
    fi
RUN cd /build/support/ptg2_scanner \
    && if [ "${TARGETARCH:-amd64}" = "amd64" ]; then \
        RUSTFLAGS="${PTG2_SCANNER_RUSTFLAGS_AMD64}" python3 -m maturin build --release --features python-extension --out /build/wheels; \
    else \
        python3 -m maturin build --release --features python-extension --out /build/wheels; \
    fi

FROM python:3.14-slim-trixie

#
WORKDIR /wheels
ADD ./requirements.txt /wheels
ADD ./requirements-dev.txt /wheels

WORKDIR /opt
RUN apt-get update \
    && if apt-cache show libaio1t64 >/dev/null 2>&1; then LIBAIO_PKG=libaio1t64; else LIBAIO_PKG=libaio1; fi \
    && apt-get install -y --no-install-recommends gcc g++ pkg-config libgdal-dev nginx git curl parallel "${LIBAIO_PKG}" \
    && python3 -m venv venv \
    && . venv/bin/activate \
    && pip install --no-compile --upgrade pip \
    && pip install --no-compile -r /wheels/requirements-dev.txt -f /wheels \
    && install -d -o nobody -g root -m 700 \
        /var/lib/nginx/body \
        /var/lib/nginx/proxy \
        /var/lib/nginx/fastcgi \
        /var/lib/nginx/uwsgi \
        /var/lib/nginx/scgi \
    && rm -rf /wheels \
    && rm -rf /root/.cache/pip/* \
    && find . -name '*.pyc' -delete \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ARG HLTHPRT_LOG_CFG=./logging.yaml
ARG HLTHPRT_RELEASE="dev"
ARG HLTHPRT_ENVIRONMENT=test

ARG HLTHPRT_DB_POOL_MIN_SIZE=1
ARG HLTHPRT_DB_POOL_MAX_SIZE=10

ARG HLTHPRT_DB_HOST=localhost
ARG HLTHPRT_DB_PORT=5432
ARG HLTHPRT_DB_DATABASE=healthporta
ARG HLTHPRT_DB_SCHEMA='mrf'
ARG HLTHPRT_DB_USER=dmytro
ARG HLTHPRT_REDIS_ADDRESS=redis://localhost:6379

ARG HLTHPRT_SAVE_PER_PACK=100


ENV HLTHPRT_LOG_CFG=${HLTHPRT_LOG_CFG}
ENV HLTHPRT_RELEASE=${HLTHPRT_RELEASE}
ENV HLTHPRT_ENVIRONMENT=${HLTHPRT_ENVIRONMENT}
ENV HLTHPRT_DB_POOL_MIN_SIZE=${HLTHPRT_DB_POOL_MIN_SIZE}
ENV HLTHPRT_DB_POOL_MAX_SIZE=${HLTHPRT_DB_POOL_MAX_SIZE}

ENV HLTHPRT_DB_HOST=${HLTHPRT_DB_HOST}
ENV HLTHPRT_DB_PORT=${HLTHPRT_DB_PORT}
ENV HLTHPRT_DB_USER=${HLTHPRT_DB_USER}
ENV HLTHPRT_DB_DATABASE=${HLTHPRT_DB_DATABASE}
ENV HLTHPRT_DB_SCHEMA=${HLTHPRT_DB_SCHEMA}

ENV HLTHPRT_REDIS_ADDRESS=${HLTHPRT_REDIS_ADDRESS}
ENV HLTHPRT_SAVE_PER_PACK=${HLTHPRT_SAVE_PER_PACK}
ENV HLTHPRT_PTG2_RUST_SCANNER_BIN=/opt/support/ptg2_scanner/target/release/ptg2_scanner
ENV HLTHPRT_PTG2_RUST_REQUIRE_RELEASE=true

ADD service/nginx.conf /etc/nginx/nginx.conf
ADD service/start_api.sh /usr/local/bin/start_api.sh
ADD service/run_import.sh /usr/local/bin/run_import.sh
RUN chmod a+x /usr/local/bin/start_api.sh /usr/local/bin/run_import.sh

COPY api/ /opt/api/
COPY db/ /opt/db/
COPY data/ /opt/data/
COPY restore/ /opt/restore/
COPY specs/ /opt/specs/
COPY alembic/ /opt/alembic/
COPY process/ /opt/process/
COPY scripts/ /opt/scripts/
COPY support/ /opt/support/
COPY --from=ptg2-scanner-builder \
    /build/support/ptg2_scanner/target/release/ptg2_scanner \
    /opt/support/ptg2_scanner/target/release/ptg2_scanner
COPY --from=ptg2-scanner-builder /build/wheels/ /tmp/ptg2-address-canon-wheels/
RUN . /opt/venv/bin/activate \
    && pip install --no-compile /tmp/ptg2-address-canon-wheels/*.whl \
    && rm -rf /tmp/ptg2-address-canon-wheels
COPY logging.yaml main.py alembic.ini /opt/

EXPOSE 8080
CMD ["/usr/local/bin/start_api.sh"]
