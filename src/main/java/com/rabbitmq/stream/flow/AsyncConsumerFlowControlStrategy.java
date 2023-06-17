package com.rabbitmq.stream.flow;

/**
 * Variant of {@link ConsumerFlowControlStrategy} that implements {@link MessageHandlingListener} to asynchronously
 * mark messages as handled.
 */
public interface AsyncConsumerFlowControlStrategy extends ConsumerFlowControlStrategy, MessageHandlingListener {

}
