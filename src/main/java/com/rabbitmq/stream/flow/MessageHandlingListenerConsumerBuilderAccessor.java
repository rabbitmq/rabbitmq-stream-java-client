package com.rabbitmq.stream.flow;

import com.rabbitmq.stream.ConsumerBuilder;

public interface MessageHandlingListenerConsumerBuilderAccessor extends ConsumerBuilder.ConsumerBuilderAccessor {
    MessageHandlingListener messageHandlingListener();
}