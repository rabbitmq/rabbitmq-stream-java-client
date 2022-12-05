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

cat > ${PWD}/enabled_plugins << ENDOFFILE
[rabbitmq_stream,rabbitmq_mqtt,rabbitmq_stomp].
ENDOFFILE

echo "Running RabbitMQ ${RABBITMQ_IMAGE}:${RABBITMQ_IMAGE_TAG}"

docker rm -f rabbitmq 2>/dev/null || echo "rabbitmq was not running"
docker run -d --name rabbitmq \
    -p 5672:5672 -p 5552:5552 -p 1883:1883 -p 61613:61613 \
    -v "${PWD}"/enabled_plugins:/etc/rabbitmq/enabled_plugins \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost' \
    "${RABBITMQ_IMAGE}":"${RABBITMQ_IMAGE_TAG}"

wait_for_message rabbitmq "Server startup complete"