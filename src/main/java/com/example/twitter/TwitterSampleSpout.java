package com.example.twitter;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSampleSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	SpoutOutputCollector collector;
	LinkedBlockingQueue<Status> queue;
	TwitterStream twitterStream;

	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keywords;

	public TwitterSampleSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret,
			String[] keywords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keywords = keywords;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		StatusListener listener = new StatusListener() {

			@Override
			public void onException(Exception arg0) {

			}

			@Override
			public void onTrackLimitationNotice(int arg0) {

			}

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onStallWarning(StallWarning arg0) {

			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {

			}
		};
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey(consumerKey)
			.setOAuthConsumerSecret(consumerSecret)
			.setOAuthAccessToken(accessToken)
			.setOAuthAccessTokenSecret(accessTokenSecret);

		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.addListener(listener);

		if (keywords.length == 0)
			twitterStream.sample();
		else {
			FilterQuery query = new FilterQuery().track(keywords);
			twitterStream.filter(query);
		}
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null)
			Utils.sleep(50);
		else
			collector.emit(new Values(ret));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

}
