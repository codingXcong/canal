package com.alibaba.otter.canal.client.adapter.kafka.support;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Kafka Message类的序列化
 *
 * @author rewerma 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class KafkaMessageSerializer implements Serializer<byte[]> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, byte[] data) {
        return data;
    }

    @Override
    public void close() {
        // nothing to do
    }
}