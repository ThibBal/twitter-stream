package bigdata.twitter;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class WordCountBolt extends BaseRichBolt {

    private Map<Integer, HashMap<String, Integer>> results;
    private Map<String, Integer> counts;
    private long lastChunkTime;
    private long lastClearTime;
    private final long chunkTime;
    private final long clearTime;
    private int number = 1;
    private final static int NUMBER_OF_TRENDS = 20 ;
    private OutputCollector collector;
    
    public WordCountBolt(long chunkTime, long clearTime) {
        this.chunkTime = chunkTime;
        this.clearTime = clearTime;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	this.counts = new HashMap<String, Integer>();
    	this.results = new HashMap<Integer, HashMap<String, Integer>>();
        this.lastChunkTime = System.currentTimeMillis();
        this.lastClearTime = System.currentTimeMillis();
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
             
        long executionTime = (currentTime - lastChunkTime)/1000;
       
        if (executionTime > chunkTime) {
            sendResult(counts);
            lastChunkTime = currentTime;
            counts.clear();
            number++;
        }
    }
    
    private void sendResult(Map<String, Integer> counts) {
    	int minValue = 0;
    	HashMap<String, Integer> trendings = new HashMap<String, Integer>();	
        
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            int count = entry.getValue();
            String word = entry.getKey();
            if(count > minValue){
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
        System.out.println("Chunk number #"+number);
        System.out.println("Number of trendings: "+trendings.size());
        results.put(number, trendings);
        collector.emit(new Values(results));
        long currentTime = System.currentTimeMillis();
        
        if (currentTime - lastClearTime > 1000*clearTime) {
        	System.out.println("End : back to the first chunk");
            number = 0;
            lastClearTime = currentTime;
        }
    }
}