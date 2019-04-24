package org.kafka.producer;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {
	public static void main(String[] args) {
		long events = 10;
		Random r = new Random();
		String topic = "djt";
		
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "hadoop1:9092");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		
		for(long event =0;event<events;event++){
			long runtime = new Date().getTime();
			String ip = "192.168.2."+r.nextInt(255);
			String msg = ip;
			producer.send(new KeyedMessage<String, String>(topic,msg.toString()));
		}
		producer.close();
		System.out.println("send over ------------------");
	}
}
