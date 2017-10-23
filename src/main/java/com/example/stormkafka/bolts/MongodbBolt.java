package com.example.stormkafka.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongodbBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	private MongoClient mongoClient;
	private MongoDatabase mongoDB;
	private String collection;

	public String host;
	public int port;
	public String db;

	public MongodbBolt(String host, int port, String db, String collection) {
		this.host = host;
		this.port = port;
		this.db = db;
		this.collection = collection;
	}

	public void execute(Tuple tuple) {
		Document document = getDocumentForInput(tuple);
		try {
			mongoDB.getCollection(collection).insertOne(document);
			collector.ack(tuple);
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(tuple);
		}
	}

	private Document getDocumentForInput(Tuple tuple) {
		Document document = new Document();
		String content = (String) tuple.getValueByField("content");
		String[] parts = content.trim().split(" ");
		System.out.println("Received in MongoDB bolt " + content);
		try {
			// converting to BSON for MongoDB
			for(String part : parts) {
				String[] subParts = part.split(":");
				String fieldName = subParts[0];
				String value = subParts[1];
				document.append(fieldName, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return document;
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.mongoClient = new MongoClient(host, port);
		this.mongoDB = mongoClient.getDatabase(db);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {
		this.mongoClient.close();
	}

}
