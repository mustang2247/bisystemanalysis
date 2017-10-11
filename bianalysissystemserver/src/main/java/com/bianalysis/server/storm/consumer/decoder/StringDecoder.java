package com.bianalysis.server.storm.consumer.decoder;

import com.bianalysis.server.storm.consumer.MessagePack;

public class StringDecoder implements Decoder<String> {
    
    @Override
    public String decode(MessagePack entity) {
        return entity.getContent();
    }
}
