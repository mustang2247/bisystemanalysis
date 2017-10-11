package com.bianalysis.server.storm.starter;


import com.bianalysis.server.utils.TupleHelpers;
import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 短单词统计
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SmallCounterBolt extends BaseRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(SmallCounterBolt.class);

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
               LOG.info("Small word = " + counter.getKey() + ", count = " + counter.getValue());
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
