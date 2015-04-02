package bigdata.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	public static void main(String[] args) {
		Config config = new Config();
		TopologyBuilder build = new TopologyBuilder();
		build.setSpout("TwitterSpout", new TwitterSpout());
		build.setBolt("WordSplitBolt", new WordSplitBolt()).shuffleGrouping("TwitterSpout");
		build.setBolt("WordsFilterBolt", new WordsFilterBolt(5)).shuffleGrouping("WordSplitBolt");
		build.setBolt("WordCountBolt", new WordCountBolt(30, 30*60, 20)).shuffleGrouping("WordsFilterBolt");
		build.setBolt("ResultBolt", new ResultBolt()).shuffleGrouping("WordCountBolt");
        build.setBolt("FileOutputBolt", new FileOutputBolt()).shuffleGrouping("ResultBolt");
        
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Twitter trends", config, build.createTopology());		
	}

}
