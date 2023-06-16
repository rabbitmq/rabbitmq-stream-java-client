package com.rabbitmq.stream.flow;

import com.rabbitmq.stream.ConsumerBuilder;

public interface MessageHandlingListenerAware extends ConsumerBuilder.ConsumerBuilderAccessor {
    MessageHandlingAware messageHandlingListener();
}