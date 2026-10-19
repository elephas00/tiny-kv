#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"

if [[ -d /Volumes/TinyKVMemory && -w /Volumes/TinyKVMemory ]]; then
  WORK_DIR=/Volumes/TinyKVMemory/tinykv-heartbeat-test-${STAMP}
else
  WORK_DIR=/private/tmp/tinykv-heartbeat-test-${STAMP}
fi

mkdir -p "${WORK_DIR}/gocache"
LOG_FILE="${WORK_DIR}/test.log"

echo "root: ${ROOT_DIR}"
echo "work directory: ${WORK_DIR}"
echo "log file: ${LOG_FILE}"
echo "starting TestSplitRecoverManyClients3B"

START=$(date +%s)
(
  cd "${ROOT_DIR}" || exit 1
  TMPDIR="${WORK_DIR}/" \
  GOCACHE="${WORK_DIR}/gocache" \
  LOG_LEVEL=error \
  go test -count=1 -vet=off \
    -timeout=20m \
    -run '^TestSplitRecoverManyClients3B$' \
    ./kv/test_raftstore >"${LOG_FILE}" 2>&1
)
STATUS=$?
END=$(date +%s)

echo "test finished with exit code: ${STATUS}"
echo "elapsed: $((END - START)) seconds"
echo "log file: ${LOG_FILE}"
tail -40 "${LOG_FILE}"

exit ${STATUS}
