package com.bianalysis.server.storm.consumer.decoder;

import com.bianalysis.server.storm.consumer.MessagePack;

import java.nio.ByteBuffer;

public class ByteArrayDecoder implements Decoder<byte[]> {

    @Override
    public byte[] decode(MessagePack message) {
        ByteBuffer buf = message.payload();
        byte[] b = new byte[buf.remaining()];
        buf.get(b);
        return b;
    }

}
