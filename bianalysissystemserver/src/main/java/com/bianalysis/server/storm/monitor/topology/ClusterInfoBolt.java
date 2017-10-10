package com.bianalysis.server.storm.monitor.topology;


import com.bianalysis.server.storm.monitor.HttpCatClient;
import com.bianalysis.server.utils.Constants;
import com.bianalysis.server.utils.TupleHelpers;
import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({ "rawtypes", "unchecked"})
public class ClusterInfoBolt  extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoBolt.class);

    private static final long serialVersionUID = 1L;
    private transient Nimbus.Client client;
    private transient NimbusClient nimbusClient;
    private OutputCollector collector;
    private Map configMap = null;
   
    @Override
    public void prepare(Map map, TopologyContext topologycontext,
            OutputCollector outputcollector) {
        this.collector = outputcollector;
        this.configMap = map;
        initClient(configMap);
    }
    /**
     * 初始化nimbus client
     * @param map
     */
    private void initClient(Map map) {
        nimbusClient = NimbusClient.getConfiguredClient(map);
        client = nimbusClient.getClient();
    }
    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            if(nimbusClient == null){
                initClient(configMap);
            }
            getClusterInfo(client);
            collector.emit(new Values(tuple));
        }        
    }
    
    private void getClusterInfo(Nimbus.Client client) {
        try {
            ClusterSummary clusterSummary = client.getClusterInfo();
            List<SupervisorSummary> supervisorSummaryList = clusterSummary.get_supervisors();
            int totalWorkers = 0;
            int usedWorkers = 0;
            for(SupervisorSummary summary : supervisorSummaryList){
                totalWorkers += summary.get_num_workers() ;
                usedWorkers += summary.get_num_used_workers();
            }
            int freeWorkers = totalWorkers - usedWorkers;
            LOGGER.info("cluster totalWorkers = " + totalWorkers 
                    + ", usedWorkers = " + usedWorkers 
                    + ", freeWorkers  = " +  freeWorkers);
            
            HttpCatClient.sendMetric("ClusterMonitor", "freeSlots", "avg", String.valueOf(freeWorkers));
            HttpCatClient.sendMetric("ClusterMonitor", "totalSlots", "avg", String.valueOf(totalWorkers));
            
            List<TopologySummary> topologySummaryList = clusterSummary.get_topologies();
            long clusterTPS = 0l;
            for(TopologySummary topology : topologySummaryList){
                long topologyTPS = getTopologyTPS(topology, client);
                clusterTPS += topologyTPS;
                //过滤掉监控的Topology的数据
                if(topology.get_name().startsWith("ClusterMonitor")){
                    continue;
                }
                HttpCatClient.sendMetric(topology.get_name(), topology.get_name() + "-TPS", "avg", String.valueOf(topologyTPS));
            }
            HttpCatClient.sendMetric("ClusterMonitor", "ClusterEmitTPS", "avg", String.valueOf(clusterTPS));
            
        } catch (TException e) {
            initClient(configMap);
            LOGGER.error("get client info error.", e);
        }
    }
    
    /**
     * 计算某个Topology的TPS
     * @param topology
     * @param client
     * @return
     * @throws NotAliveException
     * @throws TException
     */
    protected long getTopologyTPS(TopologySummary topology, Nimbus.Client client) throws NotAliveException, TException{
        long topologyTps = 0l;
        String topologyId = topology.get_id();
        if(topologyId.startsWith("ClusterMonitor")){
            return topologyTps;
        }
        TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
        if(topologyInfo == null){
            return topologyTps;
        }
        List<ExecutorSummary> executorSummaryList = topologyInfo.get_executors();
        for(ExecutorSummary executor : executorSummaryList){
            topologyTps += getComponentTPS(executor);
        }
        LOGGER.info("topology = " + topology.get_name() + ", tps = " + topologyTps);
        return topologyTps;
    }
    
    /**
     * 计算每一个executor的TPS
     * @param executor
     * @return
     */
    private long getComponentTPS(ExecutorSummary executor) {
        long componentTps = 0l;
        if(executor == null){
            return componentTps;
        }
        String componentId = executor.get_component_id();
        
        if(Utils.isSystemId(componentId)){
            return componentTps;
        }
        if(executor.get_stats() == null){
            return componentTps;
        }

        Map<String, Map<String, Long>> emittedMap = executor.get_stats().get_emitted();
        //获取当前10分钟的数据，对应Storm UI上的window是10m的统计值
        Map<String, Long> minutesEmitted = emittedMap.get("600");
        if(minutesEmitted == null){
            return componentTps;
        }
        for(Map.Entry<String, Long> emittedEntry : minutesEmitted.entrySet()){
            if(Utils.isSystemId(emittedEntry.getKey())){
                continue;
            }
            if(executor.get_uptime_secs() >= 600){
                componentTps += emittedEntry.getValue() / 600;
            }
            //如果executor刚刚启动，启动时间从10秒后才开始计算
            if(executor.get_uptime_secs() >= 10 && executor.get_uptime_secs() < 600){
                componentTps += emittedEntry.getValue() / executor.get_uptime_secs();
            }   
        }
        LOGGER.debug("component = " + componentId + ", tps = " + componentTps);
        return componentTps;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputfieldsdeclarer) {
        outputfieldsdeclarer.declare(new Fields("monitor"));
    }
    
    @Override
    public Map getComponentConfiguration(){
         Map<String, Object> conf = new HashMap<String, Object>();
         //统计的时间间隔频率
         conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Constants.TPS_COUNTER_FREQUENCY_IN_SECONDS);
         return conf;
    }
        
}
