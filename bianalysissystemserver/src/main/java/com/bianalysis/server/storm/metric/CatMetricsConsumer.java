package com.bianalysis.server.storm.metric;

import com.bianalysis.server.storm.monitor.HttpCatClient;
import com.bianalysis.server.utils.CatMetricUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * 监听所有指标，将其转储给猫
 * Listens for all metrics, dumps them to cat
 *
 * To use, add this to your topology's configuration:
 *   conf.registerMetricsConsumer(com.dianping.cosmos.metric.CatSpoutMetricsConsumer.class, 1);
 *
 * Or edit the storm.yaml config file:
 *
 *   topology.metrics.consumer.register:
 *     - class: "com.dianping.cosmos.metric.CatSpoutMetricsConsumer"
 *       parallelism.hint: 1
 *
 */
@SuppressWarnings("rawtypes")
public class CatMetricsConsumer implements IMetricsConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatMetricsConsumer.class);
    private String stormId;

    @Override
    public void prepare(Map stormConf, Object registrationArgument,
                        TopologyContext context, IErrorReporter errorReporter) {
        stormId = context.getStormId();
    }


    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        for (DataPoint p : dataPoints) {
            try{
                if(CatMetricUtil.isCatMetric(p.name)){
                    HttpCatClient.sendMetric(getTopologyName(),
                            CatMetricUtil.getCatMetricKey(p.name), "sum", String.valueOf(p.value));
                }
            }
            catch(Exception e){
                LOGGER.warn("send metirc 2 cat error.", e);
            }
        }
    }
    
    private String getTopologyName(){
       return StringUtils.substringBefore(stormId, "-");
    }

    @Override
    public void cleanup() { 
    }
    
    public static void main(String[] args){
        CatMetricsConsumer c = new CatMetricsConsumer();
        c.stormId = "HippoUV_25-15-1410857734";
        System.out.println(c.getTopologyName());
    }
}
