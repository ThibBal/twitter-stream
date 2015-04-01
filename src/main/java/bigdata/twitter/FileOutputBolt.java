package bigdata.twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class FileOutputBolt extends BaseRichBolt {
    private SortedMap<String, Integer>counts = new TreeMap<String, Integer>(); 
    private ArrayList<SortedMap<String, Integer>> itemsList = new ArrayList<SortedMap<String, Integer>>();


	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    }
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }


    public void execute(Tuple input) {
    	int number = (Integer) input.getValueByField("number");
    	number = number - 1;
    	counts.clear();
    	
//    	if (number == 4){
//    		System.out.println("FIN DU COMPTE ON REPREND A ZERO");
//    		counts.clear();
//    	}
    	
    	File file = new File("WebContent/result"+number+".json");
        SortedMap<Integer, String> trendings = (SortedMap<Integer, String>) input.getValueByField("trendings");
        try {
        	JSONObject obj = new JSONObject();
        	
        	for (Map.Entry<Integer, String> entry : trendings.entrySet()) {
        		String word = entry.getValue();
        		Integer count = entry.getKey();
            	obj.put(word, count);
            	counts.put(word, count);
//            	if(counts.containsKey(word)) {
//            		count = counts.get(word) + count;
//            	}
              }
        	itemsList.add(number, counts);
        	System.out.println(itemsList);
			if (!file.exists()) {
				file.createNewFile();
			}
			 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			//System.out.println(counts);
			bw.write(obj.toJSONString());
			bw.close();
 
			System.out.println("Résultat enregistré !");
			
		} catch (IOException  e) {
            throw new RuntimeException("Error reading file ["+ file + "]");
		}
    }
}
