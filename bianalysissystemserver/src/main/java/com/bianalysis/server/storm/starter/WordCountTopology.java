
package com.bianalysis.server.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 * 这个拓扑结构展示了Storm的流分组和多功能。
 */
public class WordCountTopology {


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        // 随机生成句子
        builder.setSpout("Spout", new RandomSentenceSpout(), 3).setNumTasks(6);

        // 根据单词字符数大于5进行拆分
        builder.setBolt("SplitBolt", new SplitSentenceBolt(), 4
        ).shuffleGrouping("Spout").setMaxTaskParallelism(8);

        // 长单词统计
        builder.setBolt("BiggerCounter", new BigCounterBolt(), 2
        ).fieldsGrouping("SplitBolt", "bigger", new Fields("word"));

        // 短单词统计
        builder.setBolt("SmallerCounter", new SmallCounterBolt(), 2
        ).fieldsGrouping("SplitBolt", "smaller", new Fields("word"));

        // 最终单词统计
        builder.setBolt("FinalCounter", new FinalCounterBolt(), 1).
                noneGrouping("BiggerCounter").noneGrouping("SmallerCounter");

        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(5);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
