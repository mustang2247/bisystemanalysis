package com.bianalysis.server.storm.starter;

import com.bianalysis.server.utils.TupleHelpers;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 长单词统计
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class BigCounterBolt extends BaseRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(BigCounterBolt.class);

    private static final long serialVersionUID = 1L;
    
    private List<Tuple> anchors = new ArrayList<Tuple>();
    
    private Map<String, Integer> counters = new HashMap<String, Integer>();
    
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
       if (TupleHelpers.isTickTuple(tuple)) {
           for(Map.Entry<String, Integer> counter : counters.entrySet()){
               LOG.info("Big word = " + counter.getKey() + ", count = " + counter.getValue());
               collector.emit(anchors, new Values(counter.getKey(), counter.getValue()));
           }
           counters.clear();
           anchors.clear();
           return;
       }

      String word = tuple.getString(0);
      Integer count = counters.get(word);
      if (count == null){
        count = 0;
      }
      count++;
      counters.put(word, count);
      anchors.add(tuple);
      
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }

    /**
     * 在此方法的实现中可以定义以”Topology.*”开头的此bolt特定的Config
     * 设置的tick机制
     * @return
     */
    @Override
    public Map getComponentConfiguration(){
         Map<String, Object> conf = new HashMap<String, Object>();
         conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
         return conf;
    }

}
