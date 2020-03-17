# RabbitMQ Stream Java Client

## How to Use

Follow the instructions to start the [RabbitMQ Stream Plugin](https://github.com/rabbitmq/rabbitmq-stream).

Then:

```
git clone git@github.com:rabbitmq/rabbitmq-stream-java-client.git
cd rabbitmq-stream-java-client
./mvnw package -DskipTests
java -jar target/stream-perf-test.jar --help
...
java -jar target/stream-perf-test.jar
```

## Copyright and License

(c) 2020, VMware Inc or its affiliates.

Double licensed under the ASL2 and MPL1.1.
See [LICENSE](./LICENSE) for details.
