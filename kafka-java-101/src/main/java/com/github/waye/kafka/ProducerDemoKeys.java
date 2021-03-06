package com.github.waye.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
public static void main(String[] args) throws InterruptedException, ExecutionException {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		// create Producer properties
		Properties properties = new Properties();
		
//		// old ways
//		properties.setProperty("bootstrap.servers","0.0.0.0:9092");
//		properties.setProperty("key.serializer", StringSerializer.class.getName());
//		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		// new ways
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		// create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		
		for(int i=0; i<10;i++) {
			
		String topic ="first_topic";
		String value ="Hello World"+Integer.toString(i);
		String key = "Key_"+Integer.toString(i);
		
		
			
			// create a producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
		
		logger.info("key: "+key);
		
		// send data ~ asynchronous
		producer.send(record, new Callback() {
			public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				//executes every time a record is successfully sent or an exception is thrown
				if (e ==null) {
					// the record was successfully send 
					logger.info("Receive new metadata \n"+
					"Topics: "+recordMetadata.topic()+"\n"+
					"partition: "+recordMetadata.partition()+"\n"+
					"Offsets: "+recordMetadata.offset()+"\n"+
					"TimeStamp: "+recordMetadata.timestamp()
					);
					
				}else {
					logger.error("Error while produing",e);
				}
			}
		}).get();// block send() to make it synchronous. - don't do it in production.
		}
		
		
		// flush data
		producer.flush();
		// flush and close producer
		producer.close();
		
		
	}
}
