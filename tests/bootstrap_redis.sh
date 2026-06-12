#!/usr/bin/env bash

set -x

GITHUB_ENV=${GITHUB_ENV:-}

USE_SSL=${USE_SSL:-false}
SSL_PATH=${SSL_PATH:-$(pwd)/tests/ssl_config}
NODE_IMAGE=${NODE_IMAGE:-redis:latest}
CLI_COMMAND=${CLI_COMMAND:-redis-cli}
CONTAINER_PREFIX=${CONTAINER_PREFIX:-}
NODE_TYPE=${NODE_TYPE:-standalone}
NODE_PORT=${NODE_PORT:-6379}

get_container_ip() {
  local container_name=${1}
  docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${container_name}"
}

add_to_hosts() {
    local hostname=${1}
    local container_ip=${2}
    if [ -n "${GITHUB_ENV}" ]; then
         sudo sh -c "echo -ne '\n${container_ip} ${hostname}' >> /etc/hosts"
    fi
}

cli() {
  docker run -v "${SSL_PATH}:/ssl-certs" --rm "${NODE_IMAGE}" \
    "${CLI_COMMAND}" ${client_args[@]} "$@"
}

case "${USE_SSL}" in
    "true" | "1")
        client_args=( --tls --cacert "/ssl-certs/cacerts.pem" )
        get_server_args() {
            local -n args=$1
            local name=$2
            args=(
                  --port 0 --tls-port 6379 --tls-auth-clients no --tls-cert-file "/ssl-certs/${name}.pem"
                  --tls-key-file "/ssl-certs/${name}.key" --tls-ca-cert-file "/ssl-certs/cacerts.pem"
            )
        }
    ;;

    "false" | "0")
        client_args=()
        get_server_args() {
            :
        }
    ;;

    *)
        echo "invalid USE_SSL ${USE_SSL} given"
        exit 1
    ;;
esac

case "${NODE_TYPE}" in
    "standalone")
        hostname=redis
        get_server_args 'server_args' "${hostname}"
        container_name="${CONTAINER_PREFIX}standalone"
        docker run --name "${container_name}" -v "${SSL_PATH}:/ssl-certs" --rm -d \
            "${NODE_IMAGE}" ${server_args[@]}

        container_ip=$(get_container_ip "${container_name}")
        until cli -h "${container_ip}" -p "${NODE_PORT}" ping; do sleep 1; done

        add_to_hosts "${hostname}" "${container_ip}"

        echo "${hostname}" > /tmp/test_host
    ;;

    "cluster")
        for i in {1,2,3}; do
          server_args=()
          hostname=redis-${i}
          get_server_args 'server_args' "${hostname}"
          container_name="${CONTAINER_PREFIX}${i}"

          docker run --name "${container_name}" -v "${SSL_PATH}:/ssl-certs" --rm -d \
            "${NODE_IMAGE}" \
            --cluster-enabled yes --cluster-node-timeout 5000 \
            --cluster-config-file /tmp/nodes.conf --appendonly no \
            ${server_args[@]}

          container_ip=$(get_container_ip "${container_name}")
          eval "node_${i}=${container_ip}"

          add_to_hosts "${hostname}" "${container_ip}"

          until cli -h "${container_ip}" -p "${NODE_PORT}" ping; do sleep 1; done
        done

        cli --cluster create "${node_1}:${NODE_PORT}" "${node_2}:${NODE_PORT}" "${node_3}:${NODE_PORT}" \
            --cluster-replicas 0 --cluster-yes

        until [ "$(cli -h "$node_1" -p "${NODE_PORT}" cluster info | grep -c cluster_state:ok)" -ge 1 ]; do
            sleep 1
        done

        # use the first node's hostname for discovery
        echo "redis-1" > /tmp/test_host
    ;;
    *)
        echo "invalid NODE_TYPE ${NODE_TYPE} given"
        exit 1
    ;;
esac
