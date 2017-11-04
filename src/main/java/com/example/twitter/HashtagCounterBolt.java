package com.example.twitter;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class HashtagCounterBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;
	Map<String, Integer> counterMap;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		counterMap = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String key = tuple.getString(0);
		if(!counterMap.containsKey(key)) {
			counterMap.put(key, 1);
		} else {
			counterMap.put(key, counterMap.get(key) + 1);
		}
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {
		for(Map.Entry<String, Integer> entry : counterMap.entrySet()) {
			System.out.println("Result: " + entry.getKey() + " : " + entry.getValue());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
