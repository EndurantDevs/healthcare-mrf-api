FROM ghcr.io/astral-sh/uv:0.12.11@sha256:79c6f4776b851471cc73b7d21d0cc834bb94383c292e83640d27eff512864df7 AS uv-bin

FROM docker.io/library/rust:1.98.1-slim-trixie@sha256:ce84a5edd80c5f91e05c5533b1e53eb1da54028f33734dc06aa6b49fa190462d AS ptg2-scanner-builder

COPY --from=uv-bin /uv /usr/local/bin/uv

ARG TARGETARCH
ARG PTG2_SCANNER_RUSTFLAGS_AMD64="-C target-cpu=x86-64-v3"

WORKDIR /build
COPY requirements.txt requirements-runtime.in requirements-runtime.lock requirements-build.txt requirements-build.lock /build/
COPY scripts/python_locks.py /build/scripts/python_locks.py
COPY support/ptg2_scanner/ /build/support/ptg2_scanner/
COPY process/ext/address_pub28.py /build/process/ext/address_pub28.py
RUN uv venv --python 3.14.7 /build/venv \
    && /build/venv/bin/python /build/scripts/python_locks.py check --root /build \
    && uv pip install \
        --python /build/venv/bin/python \
        --no-cache \
        --no-deps \
        --only-binary=:all: \
        --require-hashes \
        -r /build/requirements-build.lock \
    && uv pip check --python /build/venv/bin/python
RUN if [ "${TARGETARCH:-amd64}" = "amd64" ]; then \
        RUSTFLAGS="${PTG2_SCANNER_RUSTFLAGS_AMD64}" cargo build --release --bins --manifest-path /build/support/ptg2_scanner/Cargo.toml; \
    else \
        cargo build --release --bins --manifest-path /build/support/ptg2_scanner/Cargo.toml; \
    fi
RUN cd /build/support/ptg2_scanner \
    && if [ "${TARGETARCH:-amd64}" = "amd64" ]; then \
        RUSTFLAGS="${PTG2_SCANNER_RUSTFLAGS_AMD64}" /build/venv/bin/python -m maturin build --release --features python-extension --out /build/wheels; \
    else \
        /build/venv/bin/python -m maturin build --release --features python-extension --out /build/wheels; \
    fi

FROM docker.io/library/python:3.14.7-slim-trixie@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6

COPY --from=uv-bin /uv /usr/local/bin/uv

#
WORKDIR /wheels
COPY requirements.txt requirements-runtime.in requirements-runtime.lock requirements-build.txt requirements-build.lock /wheels/
COPY scripts/python_locks.py /wheels/scripts/python_locks.py

WORKDIR /opt
RUN apt-get update \
    && if apt-cache show libaio1t64 >/dev/null 2>&1; then LIBAIO_PKG=libaio1t64; else LIBAIO_PKG=libaio1; fi \
    && apt-get install -y --no-install-recommends nginx git curl parallel "${LIBAIO_PKG}" \
    && uv venv --python /usr/local/bin/python3 --no-python-downloads /opt/venv \
    && python3 /wheels/scripts/python_locks.py check --root /wheels \
    && uv pip install --python /opt/venv/bin/python --no-cache --only-binary=:all: --require-hashes -r /wheels/requirements-runtime.lock \
    && uv pip check --python /opt/venv/bin/python \
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
    && rm -rf /root/.cache/uv/* \
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
# Explicit, default-off importer commands documented for image users.
COPY scripts/smoke/formulary_fhir_reviewed_operator.py /opt/scripts/smoke/formulary_fhir_reviewed_operator.py
COPY scripts/smoke/formulary_fhir_synthetic_canary.py /opt/scripts/smoke/formulary_fhir_synthetic_canary.py
COPY scripts/smoke/formulary_fhir_synthetic_seed_publisher.py /opt/scripts/smoke/formulary_fhir_synthetic_seed_publisher.py
COPY scripts/smoke/provider_directory_fhir_reviewed_subset_state.py /opt/scripts/smoke/provider_directory_fhir_reviewed_subset_state.py
COPY scripts/smoke/provider_directory_rooted_graph_operator.py /opt/scripts/smoke/provider_directory_rooted_graph_operator.py
COPY scripts/smoke/provider_directory_terminal_root_retirement.py /opt/scripts/smoke/provider_directory_terminal_root_retirement.py
COPY scripts/smoke/uhc_flex_practitioner_operator.py /opt/scripts/smoke/uhc_flex_practitioner_operator.py
COPY scripts/smoke/uhc_formulary_operator.py /opt/scripts/smoke/uhc_formulary_operator.py
COPY scripts/smoke/fixtures/formulary_fhir/canary_expected_v1.json /opt/scripts/smoke/fixtures/formulary_fhir/canary_expected_v1.json
COPY scripts/smoke/fixtures/formulary_fhir/coverage_plan.json /opt/scripts/smoke/fixtures/formulary_fhir/coverage_plan.json
COPY scripts/smoke/fixtures/formulary_fhir/medication_a.json /opt/scripts/smoke/fixtures/formulary_fhir/medication_a.json
COPY scripts/smoke/fixtures/formulary_fhir/medication_b.json /opt/scripts/smoke/fixtures/formulary_fhir/medication_b.json
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
RUN uv pip install --python /opt/venv/bin/python --no-cache --no-build --no-deps /tmp/ptg2-address-canon-wheels/*.whl \
    && uv pip check --python /opt/venv/bin/python \
    && rm -rf /tmp/ptg2-address-canon-wheels
COPY logging.yaml main.py alembic.ini /opt/

USER nobody:nogroup

EXPOSE 8080
CMD ["/usr/local/bin/start_api.sh"]
