package com.bianalysis.server.storm.sport;


import com.alibaba.fastjson.JSONObject;
import com.bianalysis.server.conf.FieldNames;
import com.bianalysis.server.redis.RedisManager;
import com.bianalysis.server.storm.blackhole.BlackholeMessageId;
import com.bianalysis.server.storm.blackhole.MessageFetcher;
import com.bianalysis.server.storm.blackhole.StormOffsetStrategy;
import com.bianalysis.server.storm.consumer.Consumer;
import com.bianalysis.server.storm.consumer.ConsumerConfig;
import com.bianalysis.server.storm.consumer.MessagePack;
import com.bianalysis.server.storm.consumer.MessageStream;
import com.bianalysis.server.utils.CatMetricUtil;
import com.bianalysis.server.utils.Constants;
import com.bianalysis.server.utils.JSONUtils;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * DAU计算入口
 */
public class DAUQueueSpout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(DAUQueueSpout.class);
    private SpoutOutputCollector collector;

    /**
     * topic名称
     */
    private String topic;
    /**
     * comsumer的group名称
     */
    private String group;
    /**
     * 多少条消息后，同步一次到Redis中
     */
    private int syncFrequency;

    /**
     * 计数 统计对象
     * reset时清零
     */
    private transient CountMetric _spoutMetric;
    private transient StormOffsetStrategy offsetStrategy;


    /**
     * 消息流
     */
    private MessageStream stream;
    /**
     * 消费者
     */
    private Consumer consumer;

    private MessageFetcher fetchThread;
    private int warnningStep = 0;

    /**
     * blackhole spout的构造函数
     * @param topic topic名称
     * @param group comsumer的group名称
     * @param syncFrequency 多少条消息后，同步offset到Redis中
     */
    public DAUQueueSpout(String topic, String group, int syncFrequency) {
        this.topic = topic;
        this.group = group;
        this.syncFrequency = syncFrequency;
    }

    /**
     * 初始化一个任务时执行
     *
     * @param conf                 Topology配置信息及storm的配置信息
     * @param topologyContext      包含该task的任务id，组件id等Topology上下文信息
     * @param spoutOutputCollector 用来输出tuple，可以保证该实例，随时（如open、close方法中）用来发送tuple，它是线程安全的
     */
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        _spoutMetric = new CountMetric();
        topologyContext.registerMetric(CatMetricUtil.getSpoutMetricName(topic, group),
                _spoutMetric, Constants.EMIT_FREQUENCY_IN_SECONDS);

        ConsumerConfig config = new ConsumerConfig();

        offsetStrategy  = new StormOffsetStrategy();
        offsetStrategy.setConsumerGroup(group);
        offsetStrategy.setSyncFrequency(syncFrequency);
        offsetStrategy.setTopic(topic);

        // 消费者
        consumer = new Consumer(topic, group, config, offsetStrategy);
        consumer.start();
        // 数据流
        stream = consumer.getStream();

        fetchThread = new MessageFetcher(stream);
        new Thread(fetchThread).start();
    }

    @Override
    public void close() {
        fetchThread.shutdown();
        offsetStrategy.syncOffset();
        super.close();
    }

    @Override
    public void activate() {
        new Thread(fetchThread).start();
    }

    @Override
    public void deactivate() {
        fetchThread.shutdown();
        offsetStrategy.syncOffset();
    }

    /**
     * Storm从Sport的输出中取出数据向下传递
     * 通常在nextTuple中输出数据
     * nextTuple方法和ack、fail在同一个线程中由storm轮询调度，因此它必须是非阻塞的
     */
    @Override
    public void nextTuple() {
        MessagePack message = fetchThread.pollMessage();
        if (message != null) {
            collector.emit(topic, new Values(message.getContent()),
                    BlackholeMessageId.getMessageId(message.getPartition(), message.getOffset()));

            _spoutMetric.incr();
            offsetStrategy.updateOffset(message);
        } else {
            Utils.sleep(100);
            warnningStep++;
            if (warnningStep % 100 == 0) {
                logger.info("Queue is empty, cannot poll message.");
            }
        }
//        try {
//            String content = RedisManager.getJedis().rpop(FieldNames.STREAM_INSTALL);
//            if (content != null && content != "") {
//                logger.info(FieldNames.STREAM_INSTALL + "  :   " + "   content:   " + content);
//                if (content == null || "nil".equals(content)) {
//                    try {
//                        Thread.sleep(300);
//                    } catch (InterruptedException e) {
//                    }
//                } else {
//                    try {
//                        JSONObject object = JSONUtils.toJSONObject(content);
//                        if (object.get("appid") == null || object.get("appid") == "" || object.get("context") == null || object.get("context") == "") {
//                            return;
//                        }
//                        collector.emit(FieldNames.STREAM_INSTALL, new Values(object.get("appid"), object.get("context")));
//                    } catch (Exception e) {
//                        logger.info(e.getMessage());
//                    }
//                }
//            }
//        } catch (Exception e) {
//            logger.info(e.getMessage());
//        }
    }

    @Override
    public void ack(Object msgId) {
        logger.debug("ack: " + msgId);

    }

    @Override
    public void fail(Object msgId) {
        logger.info("fail: " + msgId);
    }

    /**
     * 定义访问结构
     *
     * @param outputFieldsDeclarer 这里 streamid 为sql命令，appid和context等为json数据
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(FieldNames.STREAM_INSTALL,
                new Fields("appid", "context"));

//        outputFieldsDeclarer.declareStream(topic, new Fields("event"));
    }

    @Override
    public Map getComponentConfiguration(){
        Map<String, Object> conf = new HashMap<String, Object>();
        return conf;
    }

}
