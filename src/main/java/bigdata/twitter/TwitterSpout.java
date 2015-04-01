package bigdata.twitter;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		StatusListener listener = new StatusListener() {
			
			public void onStatus(Status status) {
				queue.offer(status);
			}

			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			public void onTrackLimitationNotice(int i) {
			}

			public void onScrubGeo(long l, long l1) {
			}

            public void onStallWarning(StallWarning stallWarning) {
            }

			public void onException(Exception e) {
			}
		};

        TwitterStreamFactory factory = new TwitterStreamFactory();
		twitterStream = factory.getInstance();
		twitterStream.addListener(listener);
		twitterStream.sample();
	}
	
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
        } else {
			collector.emit(new Values(ret));
		}
	}

	public void close() {
		twitterStream.shutdown();
	}
	
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	public void ack(Object id) {
	}

	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}
