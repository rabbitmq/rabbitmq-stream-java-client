package com.rabbitmq.stream.flow;

import com.rabbitmq.stream.MessageHandler;

public interface MessageHandlingListener {

    /**
     * Marks a message as handled
     *
     * @param messageContext The {@link MessageHandler.Context} of the handled message
     * @return Whether the message was marked as handled (returning {@code true})
     *         or was not found (either because it was already marked as handled, or wasn't tracked)
     */
    boolean markHandled(MessageHandler.Context messageContext);

}
