package bigdata.twitter;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    private final static int NUMBER_OF_TRENDS = 20 ;
    private OutputCollector collector;
    
    private Map<Integer, HashMap<String, Integer>> results;
    private Map<String, Integer> counts;
    private long lastChunkTime;
    private long lastClearTime;
    private int number = 1;

    public WordCountBolt(long chunkTime, long clearTime) {
        this.chunkTime = chunkTime;
        this.clearTime = clearTime;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	counts = new HashMap<String, Integer>();
    	results = new HashMap<Integer, HashMap<String, Integer>>();
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
    	int minValue = 0;
    	HashMap<String, Integer> trendings = new HashMap<String, Integer>();		
       // SortedMap<Integer, String> trendings = new TreeMap<Integer, String>();
        
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            int count = entry.getValue();
            String word = entry.getKey();
            if( count > minValue){
            	trendings.put(word, count); 
            	if(trendings.size() > NUMBER_OF_TRENDS){
            		minValue = count;
            		int secondMinValue = count;
            		String wordMinValue = word;
            		for(Map.Entry<String, Integer>trend : trendings.entrySet()){
            			if( trend.getValue() < minValue ){
            				minValue = trend.getValue();
            				wordMinValue = trend.getKey();
            			}
            		}
            		trendings.remove(wordMinValue);
            		
            		for(Map.Entry<String, Integer>trend : trendings.entrySet()){
            			if( trend.getValue() < secondMinValue ){
            				secondMinValue = trend.getValue();

            			}
            		}

            	}
            }
        }
        System.out.println("The minimum value of this chunk is: "+minValue);
        results.put(number, trendings);
        System.out.println("Chunk number #"+number);
        System.out.println("Number of trendings: "+trendings.size());
        //System.out.println("The results of this chunk : "+results);
        collector.emit(new Values(results));
        long currentTime = System.currentTimeMillis();
        
        if (currentTime - lastClearTime > clearTime * 1000) {
        	System.out.println("FIN DU COMPTE");
            lastClearTime = currentTime;            
            number = 0;
        }
    }
         
}