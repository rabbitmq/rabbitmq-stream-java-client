package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.CallbackStreamDataHandler;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;

import java.util.Map;

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
    public void handlePublishConfirm(byte publisherId, long publishingId) {
        this.callbackStreamDataHandler.handlePublishConfirm(publisherId, publishingId);
    }

    @Override
    public void handle(byte publisherId, long publishingId, short errorCode) {
        this.handlePublishError(publisherId, publishingId, errorCode);
    }

    @Override
    public void handlePublishError(byte publisherId, long publishingId, short errorCode) {
        this.callbackStreamDataHandler.handlePublishError(publisherId, publishingId, errorCode);
    }

    @Override
    public void handle(Client client, byte subscriptionId, long offset, long messageCount, long dataSize) {
        this.handleChunk(subscriptionId, offset, messageCount, dataSize);
    }

    @Override
    public void handleChunk(byte subscriptionId, long offset, long messageCount, long dataSize) {
        this.callbackStreamDataHandler.handleChunk(subscriptionId, offset, messageCount, dataSize);
    }

    @Override
    public void handle(byte subscriptionId, long offset, long chunkTimestamp, long committedChunkId, Message message) {
        this.handleMessage(subscriptionId, offset, chunkTimestamp, committedChunkId, message);
    }

    @Override
    public void handleMessage(byte subscriptionId, long offset, long chunkTimestamp, long committedChunkId, Message message) {
        this.callbackStreamDataHandler.handleMessage(subscriptionId, offset, chunkTimestamp, committedChunkId, message);
    }

    @Override
    public void handle(byte subscriptionId, short responseCode) {
        this.handleCreditNotification(subscriptionId, responseCode);
    }

    @Override
    public void handleCreditNotification(byte subscriptionId, short responseCode) {
        this.callbackStreamDataHandler.handleCreditNotification(subscriptionId, responseCode);
    }

    @Override
    public OffsetSpecification handle(Client client, byte subscriptionId, boolean active) {
        this.handleConsumerUpdate(subscriptionId, active);
        return null;
    }

    @Override
    public void handleConsumerUpdate(byte subscriptionId, boolean active) {
        this.callbackStreamDataHandler.handleConsumerUpdate(subscriptionId, active);
    }

    @Override
    public void handle(Client.ShutdownContext shutdownContext) {
        this.handleShutdown(shutdownContext);
    }

    public void handleShutdown(Client.ShutdownContext shutdownContext) {
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

    @Override
    public void handleMetadata(String stream, short code) {
        this.callbackStreamDataHandler.handleMetadata(stream, code);
    }

    @Override
    public void handleSubscribe(byte subscriptionId, String stream, OffsetSpecification offsetSpecification, Map<String, String> subscriptionProperties) {
        this.callbackStreamDataHandler.handleSubscribe(subscriptionId, stream, offsetSpecification, subscriptionProperties);
    }

    @Override
    public void handleUnsubscribe(byte subscriptionId) {
        this.callbackStreamDataHandler.handleUnsubscribe(subscriptionId);
    }

}
