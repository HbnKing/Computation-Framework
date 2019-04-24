package org.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by caoyu on 16/4/21.
 * By 中交兴路 大数据中心-基础平台部
 */
public class AA extends Thread{
    private String topic;
    private SimpleDateFormat sdf = new SimpleDateFormat("MM-dd hh:mm:ss");

    public AA(String topic){
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        Producer<String, String> producer = createProducer();
        long i = 0;
        while(true){
            i++;
            long now = System.currentTimeMillis();
            KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic,sdf.format(new Date(now))+"_"+i+"");
            producer.send(message);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer<String,String> createProducer(){
        Properties properties = new Properties();
        properties.put("metadata.broker.list","192.168.110.81:9092,192.168.110.82:9092,192.168.110.83:9092");
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("zookeeper.connect", "nnn1:2181,nnn2:2181,nslave1:2181");
        return new Producer<String, String>(new ProducerConfig(properties));
    }
}