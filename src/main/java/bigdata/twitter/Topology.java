package bigdata.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	public static void main(String[] args) {
		Config config = new Config();
		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSpout", new TwitterSpout());
        b.setBolt("WordSplitBolt", new WordSplitBolt()).shuffleGrouping("TwitterSpout");
        b.setBolt("WordsFilterBolt", new WordsFilterBolt(5)).shuffleGrouping("WordSplitBolt");
        b.setBolt("WordCountBolt", new WordCountBolt(30, 120, 20)).shuffleGrouping("WordsFilterBolt");
        b.setBolt("FileOutputBolt", new FileOutputBolt()).shuffleGrouping("WordCountBolt");
        
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Twitter trends", config, b.createTopology());

//		Runtime.getRuntime().addShutdownHook(new Thread() {
//			public void run() {
//				cluster.killTopology("Twitter trends");
//				cluster.shutdown();
//			}
//		});
		
	}

}
