package bigdata.twitter;

import java.io.File;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	public static void main(String[] args) {
		Config config = new Config();
		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("WordSplitterBolt");
        b.setBolt("WordCounterBolt", new WordCounterBolt(30, 120, 20)).shuffleGrouping("IgnoreWordsBolt");
        b.setBolt("FileOutputBolt", new FileOutputBolt()).shuffleGrouping("WordCounterBolt");
        
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Twitter trends", config, b.createTopology());

//		Runtime.getRuntime().addShutdownHook(new Thread() {
//			public void run() {
//				cluster.killTopology(TOPOLOGY);
//				cluster.shutdown();
//			}
//		});
		
	}

}
