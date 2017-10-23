package com.example.stormkafka.bolts;

import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SolrBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	SolrClient solrClient;
	String solrAddress;
	
	public SolrBolt(String solrAddress) {
		// http://localhost:8983/solr/#/collection1
		this.solrAddress = solrAddress;
	}

	public void execute(Tuple tuple) {
		SolrInputDocument document = getSolrDocumentForInput(tuple);
		try {
			solrClient.add(document);
			solrClient.commit();
			collector.ack(tuple);
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(tuple);
		}
	}

	// convert tuple to Solr document
	public SolrInputDocument getSolrDocumentForInput(Tuple tuple) {
		SolrInputDocument document = new SolrInputDocument();
		String[] parts = tuple.getValueByField("content").toString().trim().split(" ");
		try {
			for(String part : parts) {
				String[] subParts = part.split(":");
				String fieldName = subParts[0];
				String value = subParts[1];
				document.addField(fieldName, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return document;
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.solrClient = new HttpSolrClient.Builder(solrAddress).build();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
