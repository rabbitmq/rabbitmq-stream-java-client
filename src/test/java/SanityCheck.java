///usr/bin/env jbang "$0" "$@" ; exit $?
//REPOS mavencentral,ossrh-staging=https://oss.sonatype.org/content/groups/staging/,rabbitmq-packagecloud-milestones=https://packagecloud.io/rabbitmq/maven-milestones/maven2
//DEPS com.rabbitmq:stream-client:0.5.0
//DEPS org.slf4j:slf4j-simple:1.7.36

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SanityCheck {

  private static final Logger LOGGER = LoggerFactory.getLogger("rabbitmq");

  public static void main(String[] args) {

    Environment env = null;
    String s = "sanity-check-stream";
    try {
      LOGGER.info("connecting");
      env = Environment.builder().build();
      LOGGER.info("connected");
      env.streamCreator().stream(s).create();
      LOGGER.info("test stream created");
      CountDownLatch publishLatch = new CountDownLatch(1);
      Producer producer = env.producerBuilder().stream(s).build();
      LOGGER.info("producer created");
      producer.send(
          producer.messageBuilder().addData("".getBytes(StandardCharsets.UTF_8)).build(),
          confirmationStatus -> publishLatch.countDown());

      LOGGER.info("waiting for publish confirm");

      boolean done = publishLatch.await(5, TimeUnit.SECONDS);
      if (!done) {
        throw new IllegalStateException("Did not receive publish confirm");
      }

      LOGGER.info("got publish confirm");

      CountDownLatch consumeLatch = new CountDownLatch(1);
      env.consumerBuilder().stream(s)
          .offset(OffsetSpecification.first())
          .messageHandler((context, message) -> consumeLatch.countDown())
          .build();

      LOGGER.info("created consumer, waiting for message");

      done = consumeLatch.await(5, TimeUnit.SECONDS);
      if (!done) {
        throw new IllegalStateException("Did not receive message");
      }

      LOGGER.info("got message");

      LOGGER.info(
          "Test succeeded with Stream Client {}",
          Environment.class.getPackage().getImplementationVersion());
      System.exit(0);
    } catch (Exception e) {
      LOGGER.info(
          "Test failed with Stream Client {}",
          Environment.class.getPackage().getImplementationVersion(),
          e);
      System.exit(1);
    } finally {
      if (env != null) {
        env.deleteStream(s);
        env.close();
      }
    }
  }
}
