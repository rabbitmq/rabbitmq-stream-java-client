#!/usr/bin/env bash

LOCAL_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

RABBITMQ_IMAGE_TAG=${RABBITMQ_IMAGE_TAG:-3.11}
RABBITMQ_IMAGE=${RABBITMQ_IMAGE:-rabbitmq}

wait_for_message() {
  while ! docker logs "$1" | grep -q "$2";
  do
      sleep 5
      echo "Waiting 5 seconds for $1 to start..."
  done
}

make -C "${PWD}"/tls-gen/basic

mkdir rabbitmq-configuration

echo "[rabbitmq_stream,rabbitmq_mqtt,rabbitmq_stomp]." >> rabbitmq-configuration/enabled_plugins

echo "loopback_users = none

listeners.ssl.default = 5671

ssl_options.cacertfile = ${PWD}/tls-gen/basic/result/ca_certificate.pem
ssl_options.certfile   = ${PWD}/tls-gen/basic/result/server_$(hostname)_certificate.pem
ssl_options.keyfile    = ${PWD}/tls-gen/basic/result/server_$(hostname)_key.pem
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = false

stream.listeners.ssl.1 = 5551" >> rabbitmq-configuration/rabbitmq.conf


echo "Running RabbitMQ ${RABBITMQ_IMAGE}:${RABBITMQ_IMAGE_TAG}"

docker rm -f rabbitmq 2>/dev/null || echo "rabbitmq was not running"
docker run -d --name rabbitmq \
    -p 5672:5672 -p 5552:5552 -p 1883:1883 -p 61613:61613 \
    -v "${PWD}"/rabbitmq-configuration:/etc/rabbitmq \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost' \
    "${RABBITMQ_IMAGE}":"${RABBITMQ_IMAGE_TAG}"

wait_for_message rabbitmq "Server startup complete"

docker exec rabbitmq rabbitmq-diagnostics erlang_version
docker exec rabbitmq rabbitmqctl version