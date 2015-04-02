package bigdata.twitter;

import java.util.Map;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TweetSplitBolt extends BaseRichBolt {
   
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        String language = tweet.getUser().getLang();
        String text = tweet.getText().replaceAll("\\p{Punct}|\n|\u2026", " ").toLowerCase();
        String[] words = text.split(" ");
        
        for (String word : words) {
                collector.emit(new Values(language, word)); 
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("language", "word"));
    }
}
