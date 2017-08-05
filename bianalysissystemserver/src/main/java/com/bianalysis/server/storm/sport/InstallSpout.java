package com.bianalysis.server.storm.sport;


import com.bianalysis.server.conf.FieldNames;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class InstallSpout extends BaseRichSpout {

    private static final Logger logger = LoggerFactory.getLogger(InstallSpout.class);
    public static final String LOG_CHANNEL = "log";

    private SpoutOutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.LOG_ENTRY));
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
//        String content = jedis.rpop(LOG_CHANNEL);
//        if(content==null || "nil".equals(content)) {
//            try { Thread.sleep(300); } catch (InterruptedException e) {}
//        } else {
//            JSONObject obj=(JSONObject) JSONValue.parse(content);
//            LogEntry entry = new LogEntry(obj);
//            collector.emit(new Values(entry));
//        }
    }
}
