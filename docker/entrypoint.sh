#!/bin/sh
set -e

ARGS="--loadmodule /usr/local/lib/libvalkey_flash.so --flash.path ${FLASH_PATH}"

[ -n "${FLASH_CAPACITY_BYTES}" ]    && ARGS="${ARGS} --flash.capacity-bytes ${FLASH_CAPACITY_BYTES}"
[ -n "${FLASH_SYNC}" ]              && ARGS="${ARGS} --flash.sync ${FLASH_SYNC}"
[ -n "${FLASH_IO_THREADS}" ]        && ARGS="${ARGS} --flash.io-threads ${FLASH_IO_THREADS}"
[ -n "${FLASH_IO_URING_ENTRIES}" ]  && ARGS="${ARGS} --flash.io-uring-entries ${FLASH_IO_URING_ENTRIES}"

exec valkey-server $ARGS "$@"
