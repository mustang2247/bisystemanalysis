package com.bianalysis.server.storm.dau;

import com.alibaba.fastjson.JSONObject;
import com.bianalysis.server.conf.FieldNames;
import com.bianalysis.server.utils.JSONUtils;
import com.bianalysis.server.utils.TupleHelpers;
import com.twitter.heron.api.tuple.Values;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将PartialUVBolt的数据聚合起来，根据不同的type计算当前周期的数据汇总，
 * 汇总完毕的数据发射到PersistenceUVBolt中 4
 */
public class DataAggregatorUVBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(DataAggregatorUVBolt.class);

    private OutputCollector collector;

    private List<Tuple> anchors = new ArrayList<Tuple>();
    private Map<String, Integer> counts = new HashMap<String, Integer>();
    private long totalCount = 0l;

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
            for(Map.Entry<String, Integer> counter : counts.entrySet()){
                logger.info("count key = " + counter.getKey() + ", count = " + counter.getValue());
                collector.emit(anchors, new Values(counter.getKey(), counter.getValue()));
            }
            counts.clear();
            anchors.clear();
            return;
        }

        String key = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        Integer singleCount = counts.get(key);
        if (singleCount == null){
            singleCount = 0;
        }

        singleCount += count;
        counts.put(key, singleCount);
        anchors.add(tuple);

        totalCount += count;
        logger.info("Final word = " + key + ", count = " + count + ", totalCount = " + totalCount);

//        collector.emit(new Values(totalCount));
        collector.ack(tuple);
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
