package com.bianalysis.server.storm.storage;

import com.bianalysis.server.utils.Util;

import java.nio.ByteBuffer;

public class Message {
    final private byte VERSION = 1;
    private ByteBuffer buffer;
    
    public Message(ByteBuffer buf) {
        buffer = buf;
    }
    
    public Message(byte[] data) {
        buffer = ByteBuffer.allocate(headSize() + data.length);
        buffer.put(VERSION);
        buffer.putLong(Util.getCRC32(data));
        buffer.put(data);
        buffer.rewind();
    }
    
    public int getSize() {
        return 4 + buffer.capacity();
    }
    
    int headSize() {
        return (Byte.SIZE + Long.SIZE)/8;
    }
    
    public void write(ByteBuffer serBuffer) {
        serBuffer.putInt(buffer.limit());
        serBuffer.put(buffer);
    }
    
    public int payloadSize() {
        return buffer.capacity() - headSize();
    }
    
    public ByteBuffer payload() {
        ByteBuffer buf = buffer.duplicate();
        buf.position(headSize());
        return buf.slice();
    }

    private long checksum() {
        ByteBuffer buf = buffer.duplicate();
        buf.position(Byte.SIZE/8);
        return buf.getLong();
    }
    
    public boolean valid() {
        return checksum() == Util.getCRC32(buffer.array(), buffer.arrayOffset() + headSize(), payloadSize()); 
    }
    
    public String getContent() {
        ByteBuffer buf = this.payload();
        byte[] b = new byte[buf.remaining()];
        buf.get(b);
        return Util.fromBytes(b);
    }
}
