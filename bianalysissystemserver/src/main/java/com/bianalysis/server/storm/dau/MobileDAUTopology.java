package com.bianalysis.server.storm.dau;

import com.bianalysis.server.utils.Constants;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DAU计算
 */
public class MobileDAUTopology {
    private static final Logger logger = LoggerFactory.getLogger(MobileDAUTopology.class);

    private static final int TOPOLOGY_NAME_INDEX = 0;

    private static final String DAU_TOPIC_NAME = "DAU_TOPIC_NAME_MAIN"; //topic名称
    private static final String DAU_MOBILE_MAIN_SPOUT_ID = "DAU_MOBILE_MAIN_SPOUT_ID";    //sport id
    private static final String DATA_PARSER_ID = "DATA_PARSER_ID";    //数据分组id
    private static final String UDID_ID = "UDID_ID";   //设备统计id

    public MobileDAUTopology() {

    }

    public static void main(String[] args) throws Exception {

        logger.info("初始化DAU Topic……………………");
        TopologyBuilder builder = new TopologyBuilder();
        String TopologyName = getTopologyName(args);

        // 拉取数据
        builder.setSpout(DAU_MOBILE_MAIN_SPOUT_ID,
                new DAUQueueSpout(DAU_TOPIC_NAME, getTopologyName(args), 2), 1);

        // 解析数据并分组
        builder.setBolt(DATA_PARSER_ID, new DataParserBolt(),
                20).shuffleGrouping(DAU_MOBILE_MAIN_SPOUT_ID, DAU_TOPIC_NAME);

        // 设备处理
        builder.setBolt(UDID_ID, new PlatformBolt(),
                24).fieldsGrouping(DATA_PARSER_ID, new Fields("trainId", "deviceId"));

        // 统计不同设备的出现次数（UV）
        builder.setBolt(Constants.PARTIAL_UV_ID, new UVBolt("APP"),
                16).fieldsGrouping(UDID_ID, new Fields("trainId", "dpid"));

        // 数据聚合
        builder.setBolt(Constants.AGGREGATOR_UV_ID, new AggregatorUVBolt(),
                1).noneGrouping(Constants.PARTIAL_UV_ID);

        // 数据持久化
        builder.setBolt(Constants.PERSISTENCE_UV_ID, new PersistenceUVBolt("APP",
                TopologyName), 2).shuffleGrouping(Constants.AGGREGATOR_UV_ID);

        Config conf = new Config();

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);
//            conf.registerMetricsConsumer(backtype.storm.metric.LoggingMetricsConsumer.class, 1);
//            conf.registerMetricsConsumer(com.dianping.cosmos.metric.CatMetricsConsumer.class, 1);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

            logger.info("初始化DAU Topic…………………… 成功……线上");
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("MobileUV", conf, builder.createTopology());

            logger.info("初始化DAU Topic…………………… 成功……本地");
        }
    }

    /**
     * 获取拓扑名字
     *
     * @param args
     * @return
     */
    private static String getTopologyName(String[] args) {
        try {
            return args[TOPOLOGY_NAME_INDEX];
        } catch (Exception e) {
            return "MovileDAU";
        }
    }


}
