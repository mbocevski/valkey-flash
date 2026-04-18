#!/bin/sh
set -e

# Any args passed via docker compose `command:` (e.g. cluster flags) are in "$@".
# Save them before set -- overwrites the positional parameters.
PASS_THROUGH="$*"

set -- --loadmodule /usr/local/lib/libvalkey_flash.so --flash.path "${FLASH_PATH}"

[ -n "${FLASH_CAPACITY_BYTES}" ]           && set -- "$@" --flash.capacity-bytes "${FLASH_CAPACITY_BYTES}"
[ -n "${FLASH_CACHE_SIZE_BYTES}" ]         && set -- "$@" --flash.cache-size-bytes "${FLASH_CACHE_SIZE_BYTES}"
[ -n "${FLASH_SYNC}" ]                     && set -- "$@" --flash.sync "${FLASH_SYNC}"
[ -n "${FLASH_IO_THREADS}" ]               && set -- "$@" --flash.io-threads "${FLASH_IO_THREADS}"
[ -n "${FLASH_IO_URING_ENTRIES}" ]         && set -- "$@" --flash.io-uring-entries "${FLASH_IO_URING_ENTRIES}"
[ -n "${FLASH_COMPACTION_INTERVAL_SEC}" ]  && set -- "$@" --flash.compaction-interval-sec "${FLASH_COMPACTION_INTERVAL_SEC}"

# shellcheck disable=SC2086  -- word-split is intentional for simple flag tokens
exec valkey-server "$@" $PASS_THROUGH
