package com.bianalysis.server.storm.blackhole;

import com.bianalysis.server.storm.consumer.MessagePack;
import com.bianalysis.server.storm.consumer.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 消息提取器
 */
public class MessageFetcher implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(MessageFetcher.class);
    private final int MAX_QUEUE_SIZE = 1000;
    private final int TIME_OUT = 5000;

    private BlockingQueue<MessagePack> emitQueue;
    private MessageStream stream;

    private volatile boolean running;
    public MessageFetcher(MessageStream stream) {
        this.running = true;
        this.stream = stream;
        this.emitQueue = new LinkedBlockingQueue<MessagePack>(MAX_QUEUE_SIZE);
    }
    
    @Override
    public void run() {
        while (running) {
            for (MessagePack message : stream) {
                try {
                    while(!emitQueue.offer(message, TIME_OUT, TimeUnit.MILLISECONDS)) {
                        LOG.error("Queue is full, cannot offer message.");
                    }
                } catch (InterruptedException e) {
                    LOG.error("Thread Interrupted");
                    running = false;
                }
            }
        }
    }
    
    public MessagePack pollMessage() {
        return emitQueue.poll();
    }
    
    public void shutdown() {
        this.running = false;
    }
}
