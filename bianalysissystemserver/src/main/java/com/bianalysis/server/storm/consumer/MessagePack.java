package com.bianalysis.server.storm.consumer;

import com.bianalysis.server.storm.storage.Message;
import com.bianalysis.server.storm.storage.MessageAndOffset;
import com.bianalysis.server.utils.Util;

import java.nio.ByteBuffer;

public class MessagePack {
    private final long offset;
    private final String partition;
    private final Message message;

    public MessagePack(MessageAndOffset messageAndOffset, String partition) {
        this.offset = messageAndOffset.getOffset();
        this.partition = partition;
        this.message = messageAndOffset.getMessage();
    }

    public ByteBuffer payload() {
        return message.payload();
    }

    public long getOffset() {
        return offset;
    }

    public String getPartition() {
        return partition;
    }
    
    public String getContent() {
        ByteBuffer buf = payload();
        byte[] b = new byte[buf.remaining()];
        buf.get(b);
        return Util.fromBytes(b);
    }
}
