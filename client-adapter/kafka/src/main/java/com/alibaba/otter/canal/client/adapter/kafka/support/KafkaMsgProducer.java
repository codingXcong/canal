package com.alibaba.otter.canal.client.adapter.kafka.support;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka生产着
 * @author codingxcong
 * @date 2022-09-30
 */
public class KafkaMsgProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMsgProducer.class);

    private Producer<String, byte[]> producer;
    private String servers;

    public KafkaMsgProducer(String servers) {
        this.servers = servers;
        this.init();
    }

    public void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("batch.size", 16384);
        props.put("max.request.size", 1048576);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", KafkaMessageSerializer.class);

        producer = new KafkaProducer<String, byte[]>(props);
    }

    public void stop() {
        try {
            logger.info("## stop the kafka producer");
            if (producer != null) {
                producer.close();
            }
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }


    public void send(MQDestination destination, List<FlatMessage> flatMessages) {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        flatMessages.forEach(flatMessage -> {
            records.add(new ProducerRecord<>(destination.getTopic(), destination.getPartition(), null, JSON.toJSONBytes(flatMessage,
                    SerializerFeature.WriteMapNullValue)));
        });
        this.produce(records);
    }

    private List<Future> produce(List<ProducerRecord<String, byte[]>> records) {
        List<Future> futures = new ArrayList<>();
        for (ProducerRecord record : records) {
            futures.add(producer.send(record));
        }
        return futures;
    }



}
