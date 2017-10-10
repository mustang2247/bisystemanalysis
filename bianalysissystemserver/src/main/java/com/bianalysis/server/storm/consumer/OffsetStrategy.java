package com.bianalysis.server.storm.consumer;

public interface OffsetStrategy {
    
    /**
     * return the consumed offset user-defined.
     * @param topic
     * @param partitionId
     * @return
     */
    public long getOffset(String topic, String partitionId, long endOffset, long committedOffset);
}
