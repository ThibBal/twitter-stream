package bigdata.twitter;

import java.io.File;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	static final String TOPOLOGY = "Twitter trending topics";

	public static void main(String[] args) {
		Config config = new Config();
		// Changer avec l'adresse du fichier o� stocker les informations
//		File file = new File("C:/Users/Thibault/workspace/.metadata/.plugins/org.eclipse.wst.server.core/tmp0/wtpwebapps/storm-twitter-word-count/result.json");
		File file = new File("result.json");
		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("WordSplitterBolt");
        b.setBolt("WordCounterBolt", new WordCounterBolt(60, 20*60, 20)).shuffleGrouping("IgnoreWordsBolt");
        b.setBolt("FileOutputBolt", new FileOutputBolt(file)).shuffleGrouping("WordCounterBolt");
        
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			
			public void run() {
				cluster.killTopology(TOPOLOGY);
				cluster.shutdown();
			}
		});
		
	}

}
