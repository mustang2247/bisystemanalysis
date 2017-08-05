package com.bianalysis.server.storm.sport;


import com.alibaba.fastjson.JSONObject;
import com.bianalysis.server.conf.FieldNames;
import com.bianalysis.server.repo.RedisManager;
import com.bianalysis.server.utils.JSONUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 设备安装
 */
public class InstallSpout extends BaseRichSpout {

    private static final Logger logger = LoggerFactory.getLogger(InstallSpout.class);

    private SpoutOutputCollector collector;

    /**
     * 定义访问结构
     *
     * @param outputFieldsDeclarer 这里 streamid 为sql命令，appid和context等为json数据
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(FieldNames.STREAM_INSTALL,
                new Fields("appid", "context"));
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
    }

    /**
     * Storm从Sport的输出中取出数据向下传递
     * 通常在nextTuple中输出数据
     * nextTuple方法和ack、fail在同一个线程中由storm轮询调度，因此它必须是非阻塞的
     */
    @Override
    public void nextTuple() {
        try {
            String content = RedisManager.getJedis().rpop(FieldNames.STREAM_INSTALL);
            if (content != null && content != "") {
                logger.info(FieldNames.STREAM_INSTALL + "  :   " + "   content:   " + content);
                if (content == null || "nil".equals(content)) {
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                    }
                } else {
                    try {
                        JSONObject object = JSONUtils.toJSONObject(content);
                        collector.emit(FieldNames.STREAM_INSTALL, new Values(object.get("appid"), object.get("context")));
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }
}
