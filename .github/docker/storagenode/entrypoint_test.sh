#!/bin/bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT

mock_binary="${test_dir}/storagenode"
result_file="${test_dir}/result"

printf '%s\n' \
    '#!/bin/bash' \
    'printf "STORJ_TEST_CUSTOM=%s\n" "${STORJ_TEST_CUSTOM:-}"' \
    'printf "STORJ_CONSOLE_ADDRESS=%s\n" "${STORJ_CONSOLE_ADDRESS:-}"' \
    'printf "ARG=%s\n" "$@"' \
    > "${mock_binary}"
chmod 0755 "${mock_binary}"

env \
    STORAGENODE_BINARY="${mock_binary}" \
    STORJ_TEST_CUSTOM=untouched \
    RUN_PARAMS="--debug.addr=127.0.0.1:0" \
    VERSION_SERVER_URL="https://version.example.test" \
    STORAGE=4TB \
    ADDRESS="node.example.test:28967" \
    EMAIL="operator@example.test" \
    WALLET="0x1234" \
    LOG_LEVEL=debug \
    SETUP=false \
    "${script_dir}/entrypoint" --operator.wallet-features=zksync \
    > "${result_file}"

grep -Fqx 'STORJ_TEST_CUSTOM=untouched' "${result_file}"
grep -Fqx 'STORJ_CONSOLE_ADDRESS=0.0.0.0:14002' "${result_file}"
grep -Fqx 'ARG=run' "${result_file}"
grep -Fqx 'ARG=--debug.addr=127.0.0.1:0' "${result_file}"
grep -Fqx 'ARG=--config-dir' "${result_file}"
grep -Fqx 'ARG=config' "${result_file}"
grep -Fqx 'ARG=--identity-dir' "${result_file}"
grep -Fqx 'ARG=identity' "${result_file}"
grep -Fqx 'ARG=--version.server-address=https://version.example.test' "${result_file}"
grep -Fqx 'ARG=--storage.allocated-disk-space=4TB' "${result_file}"
grep -Fqx 'ARG=--contact.external-address=node.example.test:28967' "${result_file}"
grep -Fqx 'ARG=--operator.email=operator@example.test' "${result_file}"
grep -Fqx 'ARG=--operator.wallet=0x1234' "${result_file}"
grep -Fqx 'ARG=--log.level=debug' "${result_file}"
grep -Fqx 'ARG=--operator.wallet-features=zksync' "${result_file}"

env \
    STORAGENODE_BINARY="${mock_binary}" \
    VERSION_SERVER_URL= \
    STORAGE= \
    SETUP=true \
    "${script_dir}/entrypoint" \
    > "${result_file}"

grep -Fqx 'ARG=setup' "${result_file}"
if grep -Fq 'ARG=run' "${result_file}"; then
    echo 'setup mode unexpectedly invoked run' >&2
    exit 1
fi
