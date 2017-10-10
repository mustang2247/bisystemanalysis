package com.bianalysis.server.storm.consumer.decoder;

import com.dp.blackhole.consumer.api.MessagePack;

public interface Decoder<T> {

    T decode(MessagePack message);
}
