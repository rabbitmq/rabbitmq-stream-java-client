@GrabResolver(name = 'ossrh-staging', root = 'https://oss.sonatype.org/content/groups/staging/')
@GrabResolver(name = 'rabbitmq-packagecloud-milestones', root = 'https://packagecloud.io/rabbitmq/maven-milestones/maven2')
@Grab(group = 'com.rabbitmq', module = 'stream-client', version = "${version}")
@Grab(group = 'org.slf4j', module = 'slf4j-simple', version = '1.7.32')
import com.rabbitmq.stream.ConfirmationHandler
import com.rabbitmq.stream.ConfirmationStatus
import com.rabbitmq.stream.Environment
import com.rabbitmq.stream.Message
import com.rabbitmq.stream.MessageHandler
import com.rabbitmq.stream.OffsetSpecification
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

def logger = LoggerFactory.getLogger("rabbitmq")
logger.info("connecting")
def env = Environment.builder().build()
logger.info("connected")
def s = "sanity-check-stream"
env.streamCreator().stream(s).create()
logger.info("test stream created")
try {
    def publishLatch = new CountDownLatch(1)
    def producer = env.producerBuilder().stream(s).build()
    logger.info("producer created")
    producer.send(producer.messageBuilder().addData("".getBytes(StandardCharsets.UTF_8)).build(),
        new ConfirmationHandler() {
            @Override
            void handle(ConfirmationStatus confirmationStatus) {
               publishLatch.countDown()
            }
        }
    )

    logger.info("waiting for publish confirm")

    def done = publishLatch.await(5, TimeUnit.SECONDS)
    if (!done) {
        throw new IllegalStateException("Did not receive publish confirm")
    }

    logger.info("got publish confirm")

    def consumeLatch = new CountDownLatch(1)
    env.consumerBuilder()
            .stream(s)
            .offset(OffsetSpecification.first())
            .messageHandler(new MessageHandler() {
        @Override
        void handle(MessageHandler.Context context, Message message) {
            consumeLatch.countDown()
        }
    }).build()

    logger.info("created consumer, waiting for message")

    done = consumeLatch.await(5, TimeUnit.SECONDS)
    if (!done) {
        throw new IllegalStateException("Did not receive message")
    }

    logger.info("got message")

    logger.info("Test succeeded with Stream Client {}",
            Environment.package.getImplementationVersion())
    System.exit 0
} catch (Exception e) {
    logger.info("Test failed with Stream Client {}",
            Environment.package.getImplementationVersion(), e)
    System.exit 1
} finally {
    env.deleteStream(s)
    env.close();
}
