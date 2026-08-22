# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG PYTHON_IMAGE=docker.io/library/python:3.14-bookworm@sha256:ec1b01f92099324b965a51c8547d9a3b71fedc99f6991a89662e7358f9b167c9
ARG RUST_IMAGE=docker.io/library/rust:1.97.1-bookworm@sha256:705e294093973d7c10e83400393dce7b3611f8e03e55a80af7fff6d02ae1affb

FROM ${RUST_IMAGE} AS rust-toolchain

RUN rustup component add --toolchain 1.97.1 \
        clippy \
        llvm-tools-preview \
        rustfmt \
    && cargo install --locked --version 0.8.7 \
        --root /opt/cargo-tools cargo-llvm-cov \
    && cargo install --locked --version 0.22.2 \
        --root /opt/cargo-tools cargo-audit

FROM ${PYTHON_IMAGE}

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        bash \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        curl \
        git \
        jq \
        libpq-dev \
        pkg-config \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup
COPY --from=rust-toolchain /opt/cargo-tools/bin /usr/local/bin
COPY ci/arc/network_fence.py /usr/local/bin/arc-network-fence

RUN groupadd --gid 1001 runner \
    && useradd \
        --uid 1001 \
        --gid 1001 \
        --create-home \
        --shell /bin/bash \
        runner \
    && mkdir -p /home/runner/.cargo \
    && chown -R 1001:1001 /home/runner \
    && chown 0:0 /usr/local/bin/arc-network-fence \
    && chmod 0555 /usr/local/bin/arc-network-fence

ENV CARGO_HOME=/home/runner/.cargo \
    HOME=/home/runner \
    PATH=/home/runner/.cargo/bin:/home/runner/.local/bin:/usr/local/cargo/bin:${PATH} \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    RUSTUP_HOME=/usr/local/rustup

USER 1001:1001
WORKDIR /home/runner

RUN python --version \
    && cargo --version \
    && rustc --version \
    && cargo fmt --version \
    && cargo clippy --version \
    && cargo llvm-cov --version \
    && cargo audit --version \
    && test "$(rustup component list --installed | grep -c '^llvm-tools-')" = "1" \
    && git --version \
    && psql --version
