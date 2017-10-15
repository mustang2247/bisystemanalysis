package com.bianalysis.server.storm.dau;

import com.bianalysis.server.utils.TupleHelpers;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 统计不同设备的出现次数（UV）3
 * 数据不会丢失。每隔10秒会将计数值输出
 */
public class DataUVBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(DataUVBolt.class);

    private OutputCollector collector;
    private String appId;

    /**
     * tuples
     */
    private List<Tuple> anchors = new ArrayList<Tuple>();
    /**
     * 计数
     */
    private Map<String, Integer> counters = new HashMap<String, Integer>();


    public DataUVBolt(String appid) {
        this.appId = appid;
    }

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
        if (TupleHelpers.isTickTuple(tuple)) {
            for(Map.Entry<String, Integer> counter : counters.entrySet()){
                logger.info("Big word = " + counter.getKey() + ", count = " + counter.getValue());
                collector.emit(anchors, new Values(counter.getKey(), counter.getValue()));
            }
            counters.clear();
            anchors.clear();
            return;
        }
        String appid = tuple.getString(0);
        String deviceid = tuple.getString(1);
        String date = tuple.getString(2);
        String channelid = tuple.getString(3);
        String content = tuple.getString(4);

        if (appid == null || appid.isEmpty() ||
                deviceid == null || deviceid.isEmpty()) {
            return;
        }

        String key = appid + "_" + deviceid + "_" + date;
        Integer count = counters.get(key);
        if (count == null){
            count = 0;
        }
        count++;

        counters.put(key, count);
        anchors.add(tuple);

        // 回调消息
        collector.ack(tuple);

//        logger.info(FieldNames.STREAM_DAU + "  :  deviceid:    " + deviceid + appid);
//        collector.emit(FieldNames.STREAM_DAU, tuple, new Values(appid, deviceid, datatime, channelid, content));


    }

    /**
     * 定义结构
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    /**
     * 在此方法的实现中可以定义以”Topology.*”开头的此bolt特定的Config
     * 设置的tick机制
     * @return
     */
    @Override
    public Map getComponentConfiguration(){
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
        return conf;
    }
}
