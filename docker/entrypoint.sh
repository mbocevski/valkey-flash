#!/bin/sh
set -e

set -- --loadmodule /usr/local/lib/libvalkey_flash.so --flash.path "${FLASH_PATH}"

[ -n "${FLASH_CAPACITY_BYTES}" ]           && set -- "$@" --flash.capacity-bytes "${FLASH_CAPACITY_BYTES}"
[ -n "${FLASH_CACHE_SIZE_BYTES}" ]         && set -- "$@" --flash.cache-size-bytes "${FLASH_CACHE_SIZE_BYTES}"
[ -n "${FLASH_SYNC}" ]                     && set -- "$@" --flash.sync "${FLASH_SYNC}"
[ -n "${FLASH_IO_THREADS}" ]               && set -- "$@" --flash.io-threads "${FLASH_IO_THREADS}"
[ -n "${FLASH_IO_URING_ENTRIES}" ]         && set -- "$@" --flash.io-uring-entries "${FLASH_IO_URING_ENTRIES}"
[ -n "${FLASH_COMPACTION_INTERVAL_SEC}" ]  && set -- "$@" --flash.compaction-interval-sec "${FLASH_COMPACTION_INTERVAL_SEC}"

exec valkey-server "$@"
