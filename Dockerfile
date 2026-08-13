FROM docker.io/library/rust:1.97.1-slim-trixie@sha256:fc0648ac2962539be80bd424729a20fd80f7b64bfba7e90bbd642aed6c697c5a AS ptg2-scanner-builder

ARG TARGETARCH
ARG PTG2_SCANNER_RUSTFLAGS_AMD64="-C target-cpu=x86-64-v3"

WORKDIR /build
COPY requirements.txt requirements-dev.txt requirements-ci.in requirements-ci.lock /build/
COPY scripts/ci/validate_python_lock_inputs /build/scripts/ci/validate_python_lock_inputs
COPY support/ptg2_scanner/ /build/support/ptg2_scanner/
COPY process/ext/address_pub28.py /build/process/ext/address_pub28.py
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && python3 /build/scripts/ci/validate_python_lock_inputs /build \
    && grep '^maturin==1\.14\.1 --hash=sha256:' /build/requirements-ci.lock > /tmp/maturin.lock \
    && test "$(wc -l < /tmp/maturin.lock)" -eq 1 \
    && python3 -m pip install \
        --break-system-packages \
        --no-cache-dir \
        --no-deps \
        --only-binary=:all: \
        --require-hashes \
        -r /tmp/maturin.lock \
    && python3 -m pip check \
    && rm -f /tmp/maturin.lock \
    && rm -rf /var/lib/apt/lists/*
RUN if [ "${TARGETARCH:-amd64}" = "amd64" ]; then \
        RUSTFLAGS="${PTG2_SCANNER_RUSTFLAGS_AMD64}" cargo build --release --bins --manifest-path /build/support/ptg2_scanner/Cargo.toml; \
    else \
        cargo build --release --bins --manifest-path /build/support/ptg2_scanner/Cargo.toml; \
    fi
RUN cd /build/support/ptg2_scanner \
    && if [ "${TARGETARCH:-amd64}" = "amd64" ]; then \
        RUSTFLAGS="${PTG2_SCANNER_RUSTFLAGS_AMD64}" python3 -m maturin build --release --features python-extension --out /build/wheels; \
    else \
        python3 -m maturin build --release --features python-extension --out /build/wheels; \
    fi

FROM docker.io/library/python:3.14.6-slim-trixie@sha256:b921fe7e7522f828d45197a47656ec465a9b15689b27fa8e1fba2864fca5b967

#
WORKDIR /wheels
COPY requirements.txt requirements-dev.txt requirements-ci.in requirements-ci.lock /wheels/
COPY scripts/ci/install_python_lock scripts/ci/validate_python_lock_inputs /wheels/scripts/ci/

WORKDIR /opt
RUN apt-get update \
    && if apt-cache show libaio1t64 >/dev/null 2>&1; then LIBAIO_PKG=libaio1t64; else LIBAIO_PKG=libaio1; fi \
    && apt-get install -y --no-install-recommends gcc g++ pkg-config libgdal-dev nginx git curl parallel "${LIBAIO_PKG}" \
    && python3 -m venv venv \
    && . venv/bin/activate \
    && PREPUSH_PIP_REPORT=/tmp/python-lock-install-report.json /wheels/scripts/ci/install_python_lock \
    && test -x /opt/venv/bin/rapidgzip \
    && ln -sf /opt/venv/bin/rapidgzip /usr/local/bin/rapidgzip \
    && install -d -o nobody -g nogroup -m 755 /run /var/log/nginx \
    && install -d -o nobody -g nogroup -m 700 \
        /var/lib/nginx/body \
        /var/lib/nginx/proxy \
        /var/lib/nginx/fastcgi \
        /var/lib/nginx/uwsgi \
        /var/lib/nginx/scgi \
    && rm -rf /wheels \
    && rm -f /tmp/python-lock-install-report.json \
    && rm -rf /root/.cache/pip/* \
    && find . -name '*.pyc' -delete \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ARG HLTHPRT_LOG_CFG=./logging.yaml
ARG HLTHPRT_RELEASE="dev"
ARG HLTHPRT_ENVIRONMENT=test
ARG HLTHPRT_SOURCE_COMMIT

ARG HLTHPRT_DB_POOL_MIN_SIZE=1
ARG HLTHPRT_DB_POOL_MAX_SIZE=10

ARG HLTHPRT_DB_HOST=localhost
ARG HLTHPRT_DB_PORT=5432
ARG HLTHPRT_DB_DATABASE=healthporta
ARG HLTHPRT_DB_SCHEMA='mrf'
ARG HLTHPRT_DB_USER=dmytro
ARG HLTHPRT_REDIS_ADDRESS=redis://localhost:6379

ARG HLTHPRT_SAVE_PER_PACK=100

RUN test "${#HLTHPRT_SOURCE_COMMIT}" -eq 40 \
    && printf '%s' "${HLTHPRT_SOURCE_COMMIT}" \
        | grep -Eq '^[0-9a-f]{40}$' \
    && test "${HLTHPRT_SOURCE_COMMIT}" != "0000000000000000000000000000000000000000" \
    && install -d -o root -g root -m 0555 /opt/healthporta/build-identity \
    && printf '%s\n' "${HLTHPRT_SOURCE_COMMIT}" \
        > /opt/healthporta/build-identity/healthcare-source-commit \
    && chown root:root /opt/healthporta/build-identity/healthcare-source-commit \
    && chmod 0444 /opt/healthporta/build-identity/healthcare-source-commit

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
ENV HLTHPRT_PTG2_PROVIDER_GRAPH_V4_BIN=/opt/support/ptg2_scanner/target/release/ptg2_provider_graph_v4
ENV HLTHPRT_UHC_SEMANTIC_BIN=/opt/support/ptg2_scanner/target/release/uhc_semantic_facts
ENV HLTHPRT_PTG2_RUST_REQUIRE_RELEASE=true
ENV PYTHONDONTWRITEBYTECODE=1

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
COPY public_evidence/ /opt/public_evidence/
COPY scripts/ /opt/scripts/
COPY support/ /opt/support/
COPY --from=ptg2-scanner-builder \
    /build/support/ptg2_scanner/target/release/ptg2_scanner \
    /opt/support/ptg2_scanner/target/release/ptg2_scanner
COPY --from=ptg2-scanner-builder \
    /build/support/ptg2_scanner/target/release/ptg2_provider_graph_v4 \
    /opt/support/ptg2_scanner/target/release/ptg2_provider_graph_v4
COPY --from=ptg2-scanner-builder \
    /build/support/ptg2_scanner/target/release/uhc_semantic_facts \
    /opt/support/ptg2_scanner/target/release/uhc_semantic_facts
COPY --from=ptg2-scanner-builder /build/wheels/ /tmp/ptg2-address-canon-wheels/
RUN . /opt/venv/bin/activate \
    && pip install --no-compile --no-deps /tmp/ptg2-address-canon-wheels/*.whl \
    && python -m pip check \
    && rm -rf /tmp/ptg2-address-canon-wheels
COPY logging.yaml main.py alembic.ini /opt/

USER nobody:nogroup

EXPOSE 8080
CMD ["/usr/local/bin/start_api.sh"]
