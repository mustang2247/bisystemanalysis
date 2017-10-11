package com.bianalysis.server.storm.monitor.topology;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class ClusterInfoTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setBolt("ClusterInfo", new ClusterInfoBolt(), 1);
        Config conf = new Config();
        conf.setNumWorkers(1);
        
        StormSubmitter.submitTopology("ClusterMonitor", conf, builder.createTopology());

    }
}
