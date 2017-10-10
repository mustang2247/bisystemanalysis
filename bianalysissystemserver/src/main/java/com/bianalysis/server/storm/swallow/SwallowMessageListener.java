package com.bianalysis.server.storm.swallow;

import com.dianping.cosmos.blackhole.MessageFetcher;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.BackoutMessageException;
import com.dianping.swallow.consumer.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SwallowMessageListener implements MessageListener{
    public static final Logger LOG = LoggerFactory.getLogger(MessageFetcher.class);
    private final int MAX_QUEUE_SIZE = 100;
    private final int TIME_OUT = 5000;
    private volatile boolean running = true;

    private BlockingQueue<Message> emitQueue =  new LinkedBlockingQueue<Message>(MAX_QUEUE_SIZE);

    @Override
    public void onMessage(Message message) throws BackoutMessageException {
        if (running) {
            try {
                while(!emitQueue.offer(message, TIME_OUT, TimeUnit.MILLISECONDS)) {
                    LOG.error("Queue is full, cannot offer message.");
                }
            }
            catch (InterruptedException e) {
                LOG.error("Thread Interrupted");
                running = false;
            }
        }
    }
    
    public Message pollMessage() {
        return emitQueue.poll();
    }
    
    public void shutdown() {
        this.running = false;
    }
    
}
