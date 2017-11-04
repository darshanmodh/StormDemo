package com.example.twitter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterHashtagStorm {

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
		
		Properties props = new Properties();
		props.load(TwitterHashtagStorm.class.getResourceAsStream("/twitter.properties"));
		
		String consumerKey = props.getProperty("twitter.consumer.key");
		String consumerSecret = props.getProperty("twitter.consumer.secret");
		String accessToken = props.getProperty("twitter.access.token");
		String accessTokenSecret = props.getProperty("twitter.access.token.secret");
		String keywords[] = args;
		
		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(1);
		
		TopologyBuilder tp = new TopologyBuilder();
		tp.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keywords));
		tp.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt()).shuffleGrouping("twitter-spout");
		tp.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt()).fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("TwitterHashtagStorm", config, tp.createTopology());
		
		Thread.sleep(10000);
		
		cluster.shutdown();
	}

}
