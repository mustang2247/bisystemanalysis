package com.bianalysis.server.storm.bolt;

import com.alibaba.fastjson.JSONObject;
import com.bianalysis.server.conf.FieldNames;
import com.bianalysis.server.utils.JSONUtils;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 统计安装（分渠道）(Bolt)
 */
public class RuleBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(RuleBolt.class);

    private OutputCollector collector;

    /**
     * 初始化一个任务时执行
     * 类似spout的open方法
     *
     * @param map             Topology配置信息及storm的配置信息
     * @param topologyContext 包含该task的任务id，组件id等Topology上下文信息
     * @param outputCollector 用来输出tuple，可以保证该实例，随时（如open、close方法中）用来发送tuple，它是线程安全的
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    /**
     * Storm从Sport中读取一个tuple以供处理
     * 在这里我们统计安装
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String streamID = tuple.getSourceStreamId();

        if (streamID.equals(FieldNames.STREAM_STARTUP)) {
            String appid = tuple.getStringByField("appid");
            String context = tuple.getStringByField("context");

            JSONObject obj = JSONUtils.toJSONObject(context);

            logger.info(FieldNames.STREAM_STARTUP + "  :  deviceid:    " + obj.get("deviceid"));
        }

    }

    /**
     * 定义结构
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(FieldNames.STREAM_INSTALL,
                new Fields("appid", "context"));
    }
}
