package com.bianalysis.server.storm;

import com.bianalysis.server.conf.FieldNames;
import com.bianalysis.server.storm.bolt.InstallBolt;
import com.bianalysis.server.storm.bolt.RuleBolt;
import com.bianalysis.server.storm.sport.InstallSpout;
import com.bianalysis.server.storm.sport.StartUpSpout;
import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.simulator.Simulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BiTopology {
    private static final Logger logger = LoggerFactory.getLogger(BiTopology.class);

    private TopologyBuilder builder = new TopologyBuilder();
    private Config conf = new Config();
    private Simulator cluster;

    public BiTopology() {
//        builder.setSpout("logSpout", new LogSpout(), 10);
//
//        builder.setBolt("logRules", new LogRulesBolt(), 10).shuffleGrouping(
//                "logSpout");
//        builder.setBolt("indexer", new IndexerBolt(), 10).shuffleGrouping(
//                "logRules");
//        builder.setBolt("counter", new VolumeCountingBolt(), 10).shuffleGrouping("logRules");
//        builder.setBolt("countPersistor", logPersistenceBolt, 10)
//                .shuffleGrouping("counter");

        // 设置sport
        builder.setSpout(FieldNames.STREAM_INSTALL, new InstallSpout(), 2);
        builder.setSpout(FieldNames.STREAM_STARTUP, new StartUpSpout(), 2);

        // 规则过滤
        builder.setBolt("rules_bolt", new RuleBolt(), 10)
                .fieldsGrouping(FieldNames.STREAM_INSTALL, FieldNames.STREAM_INSTALL, new Fields("appid"))
                .fieldsGrouping(FieldNames.STREAM_STARTUP, FieldNames.STREAM_STARTUP, new Fields("appid"))
        ;

        builder.setBolt(FieldNames.STREAM_INSTALL + "_BOLT", new InstallBolt(), 2)
                .fieldsGrouping(FieldNames.STREAM_INSTALL, FieldNames.STREAM_INSTALL, new Fields("appid"));

//        builder.setBolt(FieldNames.STREAM_STARTUP + "_BOLT", new StartUpBolt(), 2)
//                .shuffleGrouping("appid");

        // 配置配置文件
        // 实时计算不需要可靠消息，故关闭Acker节省通讯资源
        ///conf.setNumAckers(0);
        // 设置独立Java进程数，一般设为同spout和bolt的总tasks数量相等或更多
        // 使每个task都运行在独立的Java进程中，
        // 以避免多task集中在一个jvm里运行产生GC瓶颈
//        conf.setNumWorkers(7);
//        conf.put(ConfigConstent.REDIS_PORT_KEY, ConfigConstent.DEFAULT_JEDIS_PORT);
    }


    public TopologyBuilder getBuilder() {
        return builder;
    }

    public Simulator getLocalCluster() {
        return cluster;
    }

    public Config getConf() {
        return conf;
    }

    /**
     * 本地调试
     *
     * @param runTime
     */
    public void runLocal(int runTime) {
        conf.setDebug(true);
//        conf.put(ConfigConstent.REDIS_HOST_KEY, "localhost");
//        conf.put(CassandraBolt.CASSANDRA_HOST, "localhost:9171");
        cluster = new Simulator();
        cluster.submitTopology("bisystem-test", conf, builder.createTopology());
        if (runTime > 0) {
            Utils.sleep(runTime);
            shutDownLocal();
        }
    }

    /**
     * 关闭本地调试
     */
    public void shutDownLocal() {
        if (cluster != null) {
            cluster.killTopology("bisystem-test");
            cluster.shutdown();
        }
    }

    /**
     * 集群部署
     */
    public void runCluster(String name, String redisHost, String cassandraHost) throws InvalidTopologyException, AlreadyAliveException {
//        conf.setNumWorkers(20);
//        conf.put(ConfigConstent.REDIS_HOST_KEY, redisHost);
//        conf.put(CassandraBolt.CASSANDRA_HOST,cassandraHost);
//        StormSubmitter.submitTopology(name, conf, builder.createTopology());

        HeronSubmitter.submitTopology(name, conf, builder.createTopology());
    }

    /**
     * 初始化
     *
     * @param args
     * @throws Exception
     */
    public static void init(String[] args) throws Exception {

        BiTopology topology = new BiTopology();

        if (args != null && args.length > 1) {
            topology.runCluster(args[0], args[1], args[2]);
        } else {
            if (args != null && args.length == 1) {
                System.out.println("Running in local mode, redis ip missing for cluster run");
            }
            topology.runLocal(10000);
        }

    }

}
