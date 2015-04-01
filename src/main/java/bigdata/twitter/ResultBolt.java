package bigdata.twitter;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
	private HashMap<Integer, SortedMap<Integer, String>>results;
    private SortedMap<String, Integer>counts = new TreeMap<String, Integer>(); 

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
		results = new HashMap<Integer, SortedMap<Integer, String>>();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("result"));
    }


    public void execute(Tuple input) {
    	counts.clear();
    	HashMap<Integer, SortedMap<Integer, String>> results = (HashMap<Integer, SortedMap<Integer, String>>) input.getValueByField("results");
        //SortedMap<Integer, String> trendings = (SortedMap<Integer, String>) input.getValueByField("results");
    	JSONObject result = new JSONObject();
    	for (Map.Entry<Integer, SortedMap<Integer, String>> trendings : results.entrySet()) {
        	for (Map.Entry<Integer, String> entry : trendings.getValue().entrySet()) {
	    		String word = entry.getValue();
	    		Integer count = entry.getKey();
	    		
	        	if(counts.containsKey(word)) {
	    		count = counts.get(word) + count;
	    		}
	    		
	        	counts.put(word, count);
	    
	        	
	        	if (counts.size() > 20) {
	            	counts.remove(counts.firstKey());
	            }
          }
    	}

    		for (Map.Entry<String, Integer> trend : counts.entrySet()){
    			result.put(trend.getKey(), trend.getValue());
    		}
        	
        	System.out.println(counts);
        	

        	collector.emit(new Values(result)); 
    }
}
