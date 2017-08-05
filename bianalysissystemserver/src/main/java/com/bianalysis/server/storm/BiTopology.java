package com.bianalysis.server.storm;

import com.bianalysis.server.conf.ConfigContent;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BiTopology {
    private static final Logger logger = LoggerFactory.getLogger(BiTopology.class);

    private TopologyBuilder builder = new TopologyBuilder();
    private Config conf = new Config();
    private LocalCluster cluster;

    public BiTopology() {
//        builder.setSpout("logSpout", new LogSpout(), 10);
//
//        builder.setBolt("logRules", new LogRulesBolt(), 10).shuffleGrouping(
//                "logSpout");
//        builder.setBolt("indexer", new IndexerBolt(), 10).shuffleGrouping(
//                "logRules");
//        builder.setBolt("counter", new VolumeCountingBolt(), 10).shuffleGrouping("logRules");
//
//        builder.setBolt("countPersistor", logPersistenceBolt, 10)
//                .shuffleGrouping("counter");

        // Maybe add:
        // Stem and stop word counting per file
        // The persister for the stem analysis (need to check the counting
        // capability first on storm-cassandra)

        conf.put(ConfigContent.REDIS_PORT_KEY, ConfigContent.DEFAULT_JEDIS_PORT);
    }


    public TopologyBuilder getBuilder() {
        return builder;
    }

    public LocalCluster getLocalCluster() {
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
        conf.put(ConfigContent.REDIS_HOST_KEY, "localhost");
//        conf.put(CassandraBolt.CASSANDRA_HOST, "localhost:9171");
        cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
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
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    /**
     * 集群部署
     */
    public void runCluster(String name, String redisHost, String cassandraHost) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        conf.setNumWorkers(20);
        conf.put(ConfigContent.REDIS_HOST_KEY, redisHost);
//        conf.put(CassandraBolt.CASSANDRA_HOST,cassandraHost);
        StormSubmitter.submitTopology(name, conf, builder.createTopology());
    }

//    public static void main(String[] args) throws Exception {
//
//        BiTopology topology = new BiTopology();
//
//        if (args != null && args.length > 1) {
//            topology.runCluster(args[0], args[1], args[2]);
//        } else {
//            if (args != null && args.length == 1)
//                System.out
//                        .println("Running in local mode, redis ip missing for cluster run");
//            topology.runLocal(10000);
//        }
//
//    }

}
