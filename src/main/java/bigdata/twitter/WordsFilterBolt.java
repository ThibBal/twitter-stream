package bigdata.twitter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WordsFilterBolt extends BaseRichBolt {

    private Set<String> BLACKLIST_WORDS = new HashSet<String>(Arrays.asList(new String[] {
            "a", "http", "the", "you", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get", "about",
            "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get",
            "want", "will", "know", "good", "from", "https", "because", "people", "twitter", "follow", "www", "please"
    }));
    private Set<String> LANGUAGES = new HashSet<String>(Arrays.asList(new String[] {
        	"fr", "en", "es", "de", "it", "pt", "ko", "tr", "ru", "nl", "no", "sv", "fi", "da", "pl", "hu",
    }));    
    
    private final int minLength;
    
    public WordsFilterBolt(int minLength) {
        this.minLength = minLength;
    }
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
    	String language = (String) input.getValueByField("language");
        String word = (String) input.getValueByField("word");
        
        //if ((!BLACKLIST_WORDS.contains(word)) && (LANGUAGES.contains(language))) {
        if ((!BLACKLIST_WORDS.contains(word)) && (word.length() >= minLength) ) {
            collector.emit(new Values(word));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));        
    }
}
