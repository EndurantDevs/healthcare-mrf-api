FROM docker.io/library/rust:1.97.1-slim-trixie@sha256:fc0648ac2962539be80bd424729a20fd80f7b64bfba7e90bbd642aed6c697c5a AS ptg2-scanner-builder

ARG TARGETARCH
ARG PTG2_SCANNER_RUSTFLAGS_AMD64="-C target-cpu=x86-64-v3"

WORKDIR /build
COPY requirements.txt requirements-runtime.in requirements-runtime.lock requirements-build.txt requirements-build.lock /build/
COPY scripts/python_locks.py /build/scripts/python_locks.py
COPY support/ptg2_scanner/ /build/support/ptg2_scanner/
COPY process/ext/address_pub28.py /build/process/ext/address_pub28.py
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && python3 /build/scripts/python_locks.py check --root /build \
    && python3 -m pip install \
        --break-system-packages \
        --no-cache-dir \
        --no-deps \
        --only-binary=:all: \
        --require-hashes \
        -r /build/requirements-build.lock \
    && python3 -m pip check \
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
COPY requirements.txt requirements-runtime.in requirements-runtime.lock requirements-build.txt requirements-build.lock /wheels/
COPY scripts/python_locks.py /wheels/scripts/python_locks.py

WORKDIR /opt
RUN apt-get update \
    && if apt-cache show libaio1t64 >/dev/null 2>&1; then LIBAIO_PKG=libaio1t64; else LIBAIO_PKG=libaio1; fi \
    && apt-get install -y --no-install-recommends nginx git curl parallel "${LIBAIO_PKG}" \
    && python3 -m venv venv \
    && . venv/bin/activate \
    && python /wheels/scripts/python_locks.py check --root /wheels \
    && python -m pip install --no-cache-dir --no-compile --only-binary=:all: --require-hashes -r /wheels/requirements-runtime.lock \
    && python -m pip check \
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
ARG HLTHPRT_DB_USER=mrf_api
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
COPY scripts/provider_directory_support_contract.py /opt/scripts/provider_directory_support_contract.py
COPY scripts/validation/ptg2_v3_source_api_audit.py /opt/scripts/validation/ptg2_v3_source_api_audit.py
COPY support/hospital_price_native_validation.py /opt/support/hospital_price_native_validation.py
COPY support/zip/ /opt/support/zip/
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
