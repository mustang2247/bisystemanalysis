package com.bianalysis.server.storm.dau;

import com.alibaba.fastjson.JSONObject;
import com.bianalysis.server.conf.FieldNames;
import com.bianalysis.server.utils.JSONUtils;
import com.twitter.heron.api.tuple.Values;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 解析数据并分组 1
 */
public class DataParserBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(DataParserBolt.class);

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
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        String content = input.getString(0);
        if(content == null || content.isEmpty()) return;

        JSONObject object = JSONUtils.toJSONObject(content);
        if (object.get("appid") == null || object.get("appid") == "" || object.get("context") == null || object.get("context") == "") {
            return;
        }

        JSONObject obj = JSONUtils.toJSONObject(content);

        String appid = String.valueOf(obj.get("appid"));
        String deviceid = String.valueOf(obj.get("deviceid"));
        String date = String.valueOf(obj.get("date"));
        String channelid = String.valueOf(obj.get("channelid"));

        logger.info(FieldNames.STREAM_DAU + "  :  deviceid:    " + deviceid + appid);
        collector.emit(FieldNames.STREAM_DAU, input, new Values(appid, deviceid, date, channelid, content));

//        String streamID = input.getSourceStreamId();
//        if (streamID.equals(FieldNames.STREAM_DAU)) {
//            String appid = input.getStringByField("appid");
//            String context = input.getStringByField("context");
//
//            JSONObject obj = JSONUtils.toJSONObject(context);
//
//            logger.info(FieldNames.STREAM_STARTUP + "  :  deviceid:    " + obj.get("deviceid") + appid);
//        }

    }

    /**
     * 定义结构
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
