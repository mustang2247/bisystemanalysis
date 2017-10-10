package com.bianalysis.server.storm.consumer;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.*;
import com.dp.blackhole.protocol.data.*;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;
import com.dp.blackhole.storage.MessageSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Fetcher extends Thread {
    private final Log LOG = LogFactory.getLog(Fetcher.class);
    
    private GenClient<TransferWrap, TransferWrapNonblockingConnection, FetcherProcessor> client;
    private String groupId;
    private String consumerId; 
    private String broker;
    private final Map<String, PartitionTopicInfo> partitionMap;
    private Map<PartitionTopicInfo, Boolean> partitionBlockMap;
    private BlockingQueue<FetchedDataChunk> chunkQueue;
    private ConsumerConfig config;
    
    private ScheduledExecutorService retryPool =
            Executors.newSingleThreadScheduledExecutor();
    
    public Fetcher(String groupId, String consumerId, String broker, List<PartitionTopicInfo> partitionTopicInfos, LinkedBlockingQueue<FetchedDataChunk> queue, ConsumerConfig config) {
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.broker = broker;
        this.chunkQueue = queue;
        partitionMap = new ConcurrentHashMap<String, PartitionTopicInfo>();
        partitionBlockMap = new ConcurrentHashMap<PartitionTopicInfo, Boolean>();
        for (PartitionTopicInfo info : partitionTopicInfos) {
            partitionMap.put(info.partition, info);
            partitionBlockMap.put(info, false);
        }
        this.config = config;
        client = new GenClient(
                new FetcherProcessor(),
                new TransferWrapNonblockingConnection.TransferWrapNonblockingConnectionFactory(),
                new DataMessageTypeFactory());
    }

    public Collection<PartitionTopicInfo> getpartitionInfos() {
        return partitionMap.values();
    }
    public String getGroupId() {
        return groupId;
    }

    public void shutdown() {
        LOG.debug("shutdown the fetcher " + getName());
        partitionBlockMap.clear();
        partitionMap.clear();
        retryPool.shutdown();
        client.shutdown();
    }

    @Override
    public void run() {
        LOG.info("start " + this.toString());
        try {
            client.init(getName(), Util.getHostFromBroker(broker), Util.getPortFromBroker(broker));
        } catch (ClosedChannelException e) {
            LOG.error("ClosedChannelException catched: ", e);
        } catch (IOException e) {
            LOG.error("IOException catched: ", e);
        }
        LOG.info("stopping fetcher " + getName() + " to broker " + broker);
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("[");
        buf.append("fetch thread to ")
        .append(broker)
        .append(" with ");
        for (PartitionTopicInfo pti : partitionMap.values()) {
            buf.append(pti.topic).append("-").append(pti.getBrokerString()).append("-").append(pti.partition);
        }
        buf.append(']');
        return buf.toString();
    }
    
    class FetcherProcessor implements EntityProcessor<TransferWrap, TransferWrapNonblockingConnection> {
        
        @Override
        public void OnConnected(TransferWrapNonblockingConnection connection) {
            LOG.info("Fetcher " + this + " process connected with " + connection);
            if (config.isMultiFetch()) {
                sendMultiFetchRequest(connection);
            } else {
                for (PartitionTopicInfo info : partitionBlockMap.keySet()) {
                    sendFetchRequest(connection, info);
                }
            }
        }

        @Override
        public void OnDisconnected(TransferWrapNonblockingConnection connection) {
            LOG.info("Fetcher " + this + " disconnected but will reconnect to " + connection);
        }

        @Override
        public void receiveTimout(TransferWrap msg, TransferWrapNonblockingConnection conn) {

        }

        @Override
        public void sendFailure(TransferWrap msg, TransferWrapNonblockingConnection conn) {

        }

        @Override
        public void setNioService(NioService<TransferWrap, TransferWrapNonblockingConnection> service) {

        }

        @Override
        public void process(TransferWrap response, TransferWrapNonblockingConnection from) {
            switch (response.getType()) {
            case DataMessageTypeFactory.FetchReply:
                handleFetchReply((FetchReply) response.unwrap(), from);
                break;
            case DataMessageTypeFactory.OffsetReply:
                handleOffsetReply((OffsetReply) response.unwrap(), from);
                break;
            case DataMessageTypeFactory.MultiFetchReply:
                handleMultiFetchReply((MultiFetchReply) response.unwrap(), from);
                break;
            default:
                LOG.error("response type is undefined");
                break;
            }
        }

        /*
         * put received messages into chunkQueue, and update fetchOffset in the PartitionTopicInfo
         */
        private long enqueue(ByteBufferMessageSet messages, PartitionTopicInfo info) throws InterruptedException {
            // TODO better way? this is for fetchreply with messageSet of size 0
            if (messages == null) {
                return 0;
            }
            long size = messages.getValidSize();
            if (size > 0) {
                long fetchOffset = info.getFetchedOffset();
                chunkQueue.put(new FetchedDataChunk(messages, info, fetchOffset));
                long newFetchOffset = fetchOffset + size;
                info.updateFetchOffset(newFetchOffset);
                LOG.debug("updated fetchoffset => " + fetchOffset + " + " + size + " = " + newFetchOffset);
            }
            return size;
        }
        
        private void handleMultiFetchReply(MultiFetchReply multiFetchReply, TransferWrapNonblockingConnection from) {
            List<String> partitions = multiFetchReply.getPartitionList();
            List<MessageSet> messageSets = multiFetchReply.getMessagesList();
            List<Long> offsets = multiFetchReply.getOffsetList();
            for (int i = 0; i < partitions.size(); i++) {
                PartitionTopicInfo info = partitionMap.get(partitions.get(i));
                long offset = offsets.get(i);
                if (offset == MessageAndOffset.OFFSET_OUT_OF_RANGE) {
                    partitionBlockMap.put(info, true);
                    sendOffsetRequest(from, info);
                } else {
                    try {
                        enqueue((ByteBufferMessageSet)messageSets.get(i), info);
                    } catch (InterruptedException e) {
                        LOG.error("Oops, catch an Interrupted Exception of queue.put()," +
                                " but ignore it.", e);
                    } catch (RuntimeException e) {
                        throw e;
                    }
                }
            }
            if (!needBlocking()) {
                sendMultiFetchRequest(from);
            }
        }

        private void handleOffsetReply(OffsetReply offsetReply, TransferWrapNonblockingConnection from) {
            long resetOffset = offsetReply.getOffset();
            String topic = offsetReply.getTopic();
            String partition = offsetReply.getPartition();
            PartitionTopicInfo info = partitionMap.get(partition);
            if (resetOffset >= 0) {
                LOG.debug("adjust " + "topic: " + topic + " with offset of " + resetOffset);
                info.updateFetchOffset(resetOffset);
                info.resetConsumeOffset(resetOffset);
                partitionBlockMap.put(info, false);
            } else {
                LOG.warn("received offset " + resetOffset + " < 0, retry send offset request");
                sendOffsetRequest(from, info);
                return;
            }
            
            ConsumerConnector connector = ConsumerConnector.getInstance();
            connector.updateOffset(groupId, consumerId, topic, partition, resetOffset);
            
            if (config.isMultiFetch()) {
                if (!needBlocking()) {
                    sendMultiFetchRequest(from);
                }
            } else {
                sendFetchRequest(from, info);
            }
        }

        private void handleFetchReply(FetchReply fetchReply, TransferWrapNonblockingConnection from) {
            ByteBufferMessageSet messageSet = (ByteBufferMessageSet) fetchReply.getMessageSet();
            String partition = fetchReply.getPartition();
            PartitionTopicInfo info = partitionMap.get(partition);
            long offset = fetchReply.getOffset();
            LOG.debug("reveived fetch reply: " + offset);
            if (offset == MessageAndOffset.OFFSET_OUT_OF_RANGE) {
                sendOffsetRequest(from, info);
            } else {
                long validSize = 0;
                try {
                    validSize = enqueue(messageSet, info);
                } catch (InterruptedException e) {
                    LOG.error("Interrupted when enqueue");
                    throw new RuntimeException(e.getMessage(), e);
                }
                if (validSize > 0) {
                    sendFetchRequest(from, info);
                } else {
                    RetryTask retry = new RetryTask(from, info);
                    retryPool.schedule(retry, 1, TimeUnit.SECONDS);
                }
            }
        }

        private void sendMultiFetchRequest(TransferWrapNonblockingConnection connection) {
            List<FetchRequest> fetches = new ArrayList<FetchRequest>();
            for (PartitionTopicInfo info : partitionBlockMap.keySet()) {
                fetches.add(
                    new FetchRequest(
                        info.topic, 
                        info.partition, 
                        info.getFetchedOffset(), 
                        config.getFetchSize()
                            )
                );
            }
            connection.send(new TransferWrap(new MultiFetchRequest(fetches)));
        }

        private void sendOffsetRequest(TransferWrapNonblockingConnection from,
                PartitionTopicInfo info) {
            LOG.info("send offset request for " + info);
            long offset;
            if (OffsetRequest.SMALLES_TIME_STRING.equals(config.getAutoOffsetReset())) {
                offset = OffsetRequest.EARLIES_OFFSET;
            } else if (OffsetRequest.LARGEST_TIME_STRING.equals(config.getAutoOffsetReset())) {
                offset = OffsetRequest.LATES_OFFSET;
            } else {
                offset = OffsetRequest.LATES_OFFSET;
            }
            from.send(new TransferWrap(new OffsetRequest(info.topic, info.partition, offset)));
        }

        private void sendFetchRequest(TransferWrapNonblockingConnection from,
                PartitionTopicInfo info) {
            LOG.debug("sendFetchRequest " + info.getFetchedOffset());
            from.send(
                new TransferWrap(
                    new FetchRequest(
                            info.topic, 
                            info.partition, 
                            info.getFetchedOffset(), 
                            config.getFetchSize()))
            );
        }

        private boolean needBlocking() {
            for(Map.Entry<PartitionTopicInfo, Boolean> entry : partitionBlockMap.entrySet()) {
                if (entry.getValue()) {
                    return entry.getValue();
                }
            }
            return false;
        }
        
        class RetryTask implements Runnable {
            private TransferWrapNonblockingConnection from;
            private PartitionTopicInfo info;
            
            public RetryTask(TransferWrapNonblockingConnection from, PartitionTopicInfo info) {
                this.from = from;
                this.info = info;
            }

            @Override
            public void run() {
                sendFetchRequest(from, info);
            }
        }
    }
}
