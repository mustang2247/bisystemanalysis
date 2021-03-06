package com.bianalysis.server.storm.starter;

import com.bianalysis.server.db.redis.RedisManager;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * 最终单词统计
 */
@SuppressWarnings("rawtypes")
public class FinalCounterBolt extends BaseRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(FinalCounterBolt.class);

    private static final long serialVersionUID = 1L;
    private Map<String, Integer> counts = new HashMap<String, Integer>();
    private OutputCollector collector;

    private long totalCount = 0l;

    private static Jedis jedis;
    private String channel = "channel";

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        jedis = RedisManager.getJedis();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        Integer singleCount = counts.get(word);
        if (singleCount == null) {
            singleCount = 0;
        }

        singleCount += count;
        counts.put(word, singleCount);

        jedis.hset(channel, word, String.valueOf(singleCount));

        totalCount += count;

        LOG.info("Final word = " + word + ", count = " + count + ", totalCount = " + totalCount);

        collector.emit(new Values(totalCount));
        collector.ack(tuple);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
