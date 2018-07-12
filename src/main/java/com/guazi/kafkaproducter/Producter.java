package com.guazi.kafkaproducter;

import com.guazi.kafkaproducter.utils.IpUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author yuyongjun
 * @date 2018/7/10 22:47
 */

public class Producter {

    public static void main(String[] args) {
        String ip = IpUtils.getLocalIp4Addr();
        String brokerlist = ip+":9092,"+ip+":9093,"+ip+":9094";
        Properties props = new Properties();
//        props.put("bootstrap.servers", "10.215.82.206:9092");
        props.put("bootstrap.servers", brokerlist);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "my-repli";
//        String topic = "mytopic";
        Producer<String, String> procuder = new KafkaProducer<String,String>(props);
//        int j=0;
//        while (j<100) {
            for (int i = 1; i <= 30; i++) {
                String value = "value_" + i;
                ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, value);
                procuder.send(msg);
            }
            //列出topic的相关信息
            List<PartitionInfo> partitions = new ArrayList<PartitionInfo>() ;
            partitions = procuder.partitionsFor(topic);
            for(PartitionInfo p:partitions)
            {
                System.out.println(p);
            }
            System.out.println("send message over.");
//            j++;
//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

        procuder.close(100,TimeUnit.MILLISECONDS);

    }

}
