package com.bianalysis.server.storm.consumer;


public class CommittedOffsetStrategy implements OffsetStrategy {

    @Override
    public long getOffset(String topic, String partitionId, long endOffset, long committedOffset) {
        return committedOffset;
    }

}
