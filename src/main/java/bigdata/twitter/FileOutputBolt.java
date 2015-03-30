package bigdata.twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class FileOutputBolt extends BaseRichBolt {
    private File file;
 
    public FileOutputBolt() {
    }
   
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    public void execute(Tuple input) {
    	int number = (Integer) input.getValueByField("number");
    	File file = new File("result"+number+".json");
        SortedMap<Integer, String> trendings = (SortedMap<Integer, String>) input.getValueByField("trendings");
        try {
        	JSONObject obj = new JSONObject();
        	
        	for (Map.Entry<Integer, String> entry : trendings.entrySet()) {
            	obj.put(entry.getValue(), entry.getKey());  
              }
            
			if (!file.exists()) {
				file.createNewFile();
			}
			 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			 System.out.println(obj.toJSONString());
			bw.write(obj.toJSONString());
			bw.close();
 
			System.out.println("Fichier JSON modifié");
			
		} catch (IOException  e) {
            throw new RuntimeException("Error reading file ["+ file + "]");
		}
    }
}
