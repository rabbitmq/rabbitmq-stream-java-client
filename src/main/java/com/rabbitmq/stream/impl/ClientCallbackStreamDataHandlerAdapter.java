package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.CallbackStreamDataHandler;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;

public class ClientCallbackStreamDataHandlerAdapter implements
        CallbackStreamDataHandler,
        Client.PublishConfirmListener,
        Client.PublishErrorListener,
        Client.ChunkListener,
        Client.MessageListener,
        Client.CreditNotification,
        Client.ConsumerUpdateListener,
        Client.ShutdownListener,
        Client.MetadataListener {

    private final CallbackStreamDataHandler callbackStreamDataHandler;

    public ClientCallbackStreamDataHandlerAdapter(CallbackStreamDataHandler callbackStreamDataHandler) {
        this.callbackStreamDataHandler = callbackStreamDataHandler;
    }


    @Override
    public void handle(byte publisherId, long publishingId) {
        this.handlePublishConfirm(publisherId, publishingId);
    }

    @Override
    public void handle(byte publisherId, long publishingId, short errorCode) {
        this.handlePublishError(publisherId, publishingId, errorCode);
    }

    @Override
    public void handle(Client client, byte subscriptionId, long offset, long messageCount, long dataSize) {
        this.handleChunk(subscriptionId, offset, messageCount, dataSize);
    }

    @Override
    public void handle(byte subscriptionId, long offset, long chunkTimestamp, long committedChunkId, Message message) {
        this.handleMessage(subscriptionId, offset, chunkTimestamp, committedChunkId, message);
    }

    @Override
    public void handle(byte subscriptionId, short responseCode) {
        this.handleCreditNotification(subscriptionId, responseCode);
    }

    @Override
    public OffsetSpecification handle(Client client, byte subscriptionId, boolean active) {
        this.handleConsumerUpdate(subscriptionId, active);
        return null;
    }

    @Override
    public void handle(Client.ShutdownContext shutdownContext) {
        this.handleShutdown(shutdownContext);
    }

    void handleShutdown(Client.ShutdownContext shutdownContext) {
        if(callbackStreamDataHandler instanceof AutoCloseable) {
            try {
                ((AutoCloseable) callbackStreamDataHandler).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void handle(String stream, short code) {
        this.handleMetadata(stream, code);
    }

}
