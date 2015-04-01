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
	
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    }
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }


    public void execute(Tuple input) {
    	File file = new File("WebContent/result.json");
    	
        try {
        	JSONObject result = (JSONObject) input.getValueByField("result");
			if (!file.exists()) {
				file.createNewFile();
			}
			 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(result.toJSONString());
			bw.close();
 
			System.out.println("Result recorded!");

			
		} catch (IOException  e) {
            throw new RuntimeException("Error reading file ["+ file + "]");
		}
    }
}
