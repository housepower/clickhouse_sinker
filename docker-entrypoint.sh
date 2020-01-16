#!/usr/bin/env sh

set -eux

export TZ=${TZ:-UTC}

CONFIG_DIR="${CONFIG_DIR:-/config}"
TASK_DIR="${CONFIG_DIR}/tasks"
CPUNUM=${CPUNUM:-1}
HTTP_ADDR=${HTTP_ADDR:-"0.0.0.0:2112"}
CONFIG=${CONFIG}
TASK=${TASK}

install -d ${CONFIG_DIR}
install -d ${TASK_DIR}

cat <<EOF > ${CONFIG_DIR}/config.json
${CONFIG}
EOF

cat <<EOF > ${TASK_DIR}/task.json
${TASK}
EOF

/usr/local/bin/clickhouse_sinker -conf ${CONFIG_DIR} -cpunum ${CPUNUM} -http-addr ${HTTP_ADDR}