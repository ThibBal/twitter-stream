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

public class WordSplitterBolt extends BaseRichBolt {
    private final int minLength;
    
    private OutputCollector collector;

    public WordSplitterBolt(int minLength) {
        this.minLength = minLength;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        String language = tweet.getUser().getLang(); // langue du tweet
        String text = tweet.getText().replaceAll("\\p{Punct}|\n|\u2026", " ").toLowerCase(); // mise en miniscule et remplace la ponctuation
        String[] words = text.split(" "); // casse les phrases pour r�cup�rer les mots
        
        for (String word : words) {
            if (word.length() >= minLength) { // conserver que les tweets > minLength caract�res
                collector.emit(new Values(language, word)); 
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("language", "word"));
    }
}
