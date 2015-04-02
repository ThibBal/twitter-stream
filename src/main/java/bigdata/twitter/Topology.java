package bigdata.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	public static void main(String[] args) {
		Config config = new Config();
		TopologyBuilder build = new TopologyBuilder();
		build.setSpout("TwitterSpout", new TwitterSpout());
		build.setBolt("TweetSplitBolt", new TweetSplitBolt()).shuffleGrouping("TwitterSpout");
		build.setBolt("WordFilterBolt", new WordFilterBolt()).shuffleGrouping("TweetSplitBolt");
		build.setBolt("WordCountBolt", new WordCountBolt(180, 1800)).shuffleGrouping("WordFilterBolt");
		build.setBolt("ResultBolt", new ResultBolt()).shuffleGrouping("WordCountBolt");
        build.setBolt("FileOutputBolt", new FileOutputBolt()).shuffleGrouping("ResultBolt");
        
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Twitter trends", config, build.createTopology());		
	}

}
