# Stage 1 — build the valkey-flash shared library
FROM rust:trixie@sha256:4a7e3a0c309c9bab658e469f842711bd595fae484936bc5d605e08ca0c631bf4 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends libclang-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src/ src/

RUN cargo build --release

# Stage 2 — runtime image based on official Valkey 9.0
FROM valkey/valkey:9.0-trixie@sha256:3b55fbaa0cd93cf0d9d961f405e4dfcc70efe325e2d84da207a0a8e6d8fde4f9

COPY --from=builder /build/target/release/libvalkey_flash.so /usr/local/lib/

RUN mkdir -p /var/lib/valkey-flash

COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV FLASH_PATH=/var/lib/valkey-flash/flash.bin
ENV FLASH_CAPACITY_BYTES=""
ENV FLASH_CACHE_SIZE_BYTES=""
ENV FLASH_SYNC=""
ENV FLASH_IO_THREADS=""
ENV FLASH_IO_URING_ENTRIES=""
ENV FLASH_COMPACTION_INTERVAL_SEC=""

EXPOSE 6379

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
