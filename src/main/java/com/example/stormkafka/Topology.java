package com.example.stormkafka;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;

import com.example.stormkafka.bolts.BoltBuilder;
import com.example.stormkafka.bolts.MongodbBolt;
import com.example.stormkafka.bolts.SinkTypeBolt;
import com.example.stormkafka.bolts.SolrBolt;
import com.example.stormkafka.spout.SpoutBuilder;

public class Topology {
	
	public Properties configs;
	public SpoutBuilder spoutBuilder;
	public BoltBuilder boltBuilder;
	
	public Topology(String configFile) {
		configs = new Properties();
		try {			
			configs.load(Topology.class.getResourceAsStream("/default_config.properties"));
			boltBuilder = new BoltBuilder(configs);
			spoutBuilder = new SpoutBuilder(configs);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public void submitTopology() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
		KafkaSpout kafkaSpout = spoutBuilder.buildKafkaSpout();
		SinkTypeBolt sinkTypeBolt = boltBuilder.buildSinkTypeBolt();
		MongodbBolt mongoDBBolt = boltBuilder.buildMongodbBolt();
		SolrBolt solrBolt = boltBuilder.buildSolrBolt();
		HdfsBolt hdfsBolt = boltBuilder.buildHdfsBolt();
		
		String kafkaSpoutId = configs.getProperty("kafka-spout");
		int kafkaSpoutCount = Integer.parseInt(configs.getProperty("kafkaspout.count"));
		builder.setSpout(kafkaSpoutId, kafkaSpout, kafkaSpoutCount);
		
		String sinkTypeBoltId = configs.getProperty("sink-type-bolt");
		int sinkTypeBoltCount = Integer.parseInt(configs.getProperty("sinkbolt.count"));
		builder.setBolt(sinkTypeBoltId, sinkTypeBolt, sinkTypeBoltCount).shuffleGrouping(configs.getProperty("kafka-spout"));
		
		String componentId = configs.getProperty("sink-type-bolt");
		
		String mongodbBoltId = configs.getProperty("mongodb-bolt");
		int mongodbBoltCount = Integer.parseInt(configs.getProperty("mongodbbolt.count"));
		builder.setBolt(mongodbBoltId, mongoDBBolt, mongodbBoltCount).shuffleGrouping(componentId, "mongodb-stream");
		
		String solrBoltId = configs.getProperty("solr-bolt");
		int solrBoltCount = Integer.parseInt(configs.getProperty("solrbolt.count"));
		builder.setBolt(solrBoltId, solrBolt, solrBoltCount).shuffleGrouping(componentId, "solr-stream");
		
		String hdfsBoltId = configs.getProperty("hdfs-bolt");
		int hdfsBoltCount = Integer.parseInt(configs.getProperty("hdfsbolt.count"));
		builder.setBolt(hdfsBoltId, hdfsBolt, hdfsBoltCount).shuffleGrouping(componentId, "hdfs-stream");
		
		Config config = new Config();
		config.put("solr.zookeeper.hosts", configs.getProperty("zookeeper"));
		String topologyname = "storm-kafka-topology";
		config.setNumWorkers(1);
		StormSubmitter.submitTopology(topologyname, config, builder.createTopology());
	}
	
	public static void main(String[] args){
		String configFile;
		
		if(args.length == 0) {
			System.out.println("Missing input : config file location, using default");
			configFile = "default_config.properties";
		} else {
			configFile = args[0];
		}
		
		try {
			Topology topology = new Topology(configFile);
			topology.submitTopology();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
