package bigdata.twitter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class WordCountBolt extends BaseRichBolt {

    private final long chunkTime;
    private final long clearTime;
    private final int trendSize;

    private OutputCollector collector;
    
    private Map<Integer, SortedMap<Integer, String>> results;
    private Map<String, Integer> counts;
    private long lastChunkTime;
    private long lastClearTime;
    private int number = 1;

    public WordCountBolt(long chunkTime, long clearTime, int trendSize) {
        this.chunkTime = chunkTime;
        this.clearTime = clearTime;
        this.trendSize = trendSize;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	counts = new HashMap<String, Integer>();
    	results = new HashMap<Integer, SortedMap<Integer, String>>();
        lastChunkTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("results"));
    }

    public void execute(Tuple input) {
    	long currentTime = System.currentTimeMillis();
        String word = (String) input.getValueByField("word");

        Integer count = counts.get(word);
        if (count == null){
    	   count = 0;
        }
        count++;

        counts.put(word, count);
             
        long executionTime = (currentTime - lastChunkTime) / 1000;
       
        if (executionTime > chunkTime) {
            System.out.println("Words counted: "+counts.size());
            topList(counts);
            lastChunkTime = currentTime;
            counts.clear();
            number++;
        }
    }
    
    // calculate top list:
    private void topList(Map<String, Integer> counts) {
    			
        SortedMap<Integer, String> trendings = new TreeMap<Integer, String>();
        
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            int count = entry.getValue();
            String word = entry.getKey();
            
            trendings.put(count, word); // not good put replaces the counts egals
            
            if (trendings.size() > trendSize) {
            	trendings.remove(trendings.firstKey());
            }
        }
        
        results.put(number, trendings);
        System.out.println("Number: "+number);
        System.out.println("Number of trendings: "+trendings.size());
        collector.emit(new Values(results));
        long currentTime = System.currentTimeMillis();
        
        if (currentTime - lastClearTime > clearTime * 1000) {
        	System.out.println("FIN DU COMPTE");
            lastClearTime = currentTime;            
            number = 0;
        }
    }
         
}

//ValueComparator bvc =  new ValueComparator(counts);
//TreeMap<String,Integer> trendings = new TreeMap<String,Integer>(bvc);
//sorted_map.putAll(trends);

//for (Map.Entry<String, Integer> entry : counts.entrySet()) {
//    int count = entry.getValue();
//    String word = entry.getKey();
//
//    trendings.put(word, count); // not good put replaces the counts egals
//    // put(key, value)
//    
//    if (trendings.size() > trendSize) {
//    	trendings.remove(trendings.firstKey());
//    }
//}
//
//System.out.println(trendings);

//class ValueComparator implements Comparator<String> {
//
//    Map<String, Integer> base;
//    public ValueComparator(Map<String, Integer> base) {
//        this.base = base;
//    }
//
//    // Note: this comparator imposes orderings that are inconsistent with equals.    
//    public int compare(String a, String b) {
//        if (base.get(a) >= base.get(b)) {
//            return -1;
//        } else {
//            return 1;
//        } // returning 0 would merge keys
//    }
//}