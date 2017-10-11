package com.bianalysis.server.storm.starter;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

import java.util.Map;

/**
 * 拆分句子
 * 根据单词字符数大于5进行拆分
 */
@SuppressWarnings({"rawtypes"})
public class SplitSentenceBolt  extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            if(word.length() >= 5){
                collector.emit("bigger", input, new Values(word));
            }
            else{
                collector.emit("smaller", input,  new Values(word));
            }
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fileds = new Fields("word");
        declarer.declareStream("bigger", fileds);
        declarer.declareStream("smaller", fileds);        
    }

}
