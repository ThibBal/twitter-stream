package bigdata.twitter;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ResultBolt extends BaseRichBolt {
	private OutputCollector collector;
    private HashMap<String, Integer>counts = new HashMap<String, Integer>();
    private final static int NUMBER_OF_FINAL_TRENDS = 20 ;

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("result"));
    }

    public void execute(Tuple input) {
    	counts.clear();
    	HashMap<Integer, HashMap<String, Integer>> results = (HashMap<Integer, HashMap<String, Integer>>) input.getValueByField("results");

    	JSONObject JsonResult = new JSONObject();
    	for (Map.Entry<Integer, HashMap<String, Integer>> trendings : results.entrySet()) {
        	for (Map.Entry<String, Integer> entry : trendings.getValue().entrySet()) {
	    		String word = entry.getKey();
	    		Integer count = entry.getValue();
	    		
	        	if(counts.containsKey(word)) {
	    		count = counts.get(word) + count;
	    		}
	    		
	        	counts.put(word, count);
	   
	        	if (counts.size() > NUMBER_OF_FINAL_TRENDS) {
	        		int minValue = 100000000;
            		String wordMinValue = word;
            		for(Map.Entry<String, Integer>trend : counts.entrySet()){
            			if( trend.getValue() < minValue ){
            				minValue = trend.getValue();
            				wordMinValue = trend.getKey();
            			}
            		}
            		counts.remove(wordMinValue);            		
	            }
          }
    	}
    	System.out.println("JSON data: "+counts);
    		for (Map.Entry<String, Integer> trend : counts.entrySet()){
    			JsonResult.put(trend.getKey(), trend.getValue());
    		}     	
        	collector.emit(new Values(JsonResult)); 
    }
}
