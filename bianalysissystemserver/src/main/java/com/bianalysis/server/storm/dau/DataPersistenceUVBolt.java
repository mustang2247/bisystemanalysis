package com.bianalysis.server.storm.dau;

import com.bianalysis.server.db.redis.RedisManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * 将数据写入MySQL中 5
 * 或者缓存到Redis中
 */
public class DataPersistenceUVBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(DataPersistenceUVBolt.class);

    private OutputCollector collector;
    private String appid;
    private String topName;

    private static Jedis jedis;

    public DataPersistenceUVBolt(String appid, String topName) {
        this.appid = appid;
        this.topName = topName;
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

        jedis = RedisManager.getJedis();
    }

    /**
     * Storm从Sport中读取一个tuple以供处理
     * 在这里我们统计安装
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {


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
