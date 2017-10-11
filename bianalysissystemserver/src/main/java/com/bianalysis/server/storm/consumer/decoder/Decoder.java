package com.bianalysis.server.storm.consumer.decoder;


import com.bianalysis.server.storm.consumer.MessagePack;

public interface Decoder<T> {

    T decode(MessagePack message);
}
