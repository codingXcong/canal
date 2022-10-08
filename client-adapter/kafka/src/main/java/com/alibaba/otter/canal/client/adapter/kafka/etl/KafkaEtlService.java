package com.alibaba.otter.canal.client.adapter.kafka.etl;

import com.alibaba.otter.canal.client.adapter.kafka.config.KafkaSyncConfig;
import com.alibaba.otter.canal.client.adapter.kafka.support.FlatMessageBuilder;
import com.alibaba.otter.canal.client.adapter.kafka.support.KafkaMsgProducer;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.protocol.FlatMessage;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author codingxcong
 * @date 2022-09-28
 */
public class KafkaEtlService extends AbstractEtlService {

    private KafkaSyncConfig config;
    private KafkaMsgProducer kafkaMsgProducer;

    public KafkaEtlService(KafkaMsgProducer kafkaMsgProducer, KafkaSyncConfig config) {
        super("kafka", config);
        this.config = config;
        this.kafkaMsgProducer = kafkaMsgProducer;
    }

    public EtlResult importData(List<String> params) {
        KafkaSyncConfig.KafkaMapping mapping = config.getKafkaMapping();
        logger.info("start etl to import data to kafka topic: {}", mapping.getTopic());
        String sql = mapping.getSql();
        return importData(sql, params);
    }

    @Override
    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values, AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg) {

        KafkaSyncConfig.KafkaMapping kafkaMapping = (KafkaSyncConfig.KafkaMapping) mapping;
        Util.sqlRS(ds, sql, values, rs -> {
            try {
                List<Map<String, String>> data = new ArrayList<>();
                MQDestination canalDestination = new MQDestination();
                canalDestination.setTopic(config.getKafkaMapping().getTopic());
                canalDestination.setPartition(config.getKafkaMapping().getPartition());
                String database = config.getKafkaMapping().getDatabase();
                String table = config.getKafkaMapping().getTable();
                while (rs.next()) {
                    int cc = rs.getMetaData().getColumnCount();
                    Map<String, String> item = new HashMap<>();
                    for (int j = 1; j <= cc; j++) {
                        String columnName = rs.getMetaData().getColumnName(j);
                        Object value = rs.getObject(columnName);
                        item.put(columnName, String.valueOf(value));
                    }
                    data.add(item);
                    impCount.incrementAndGet();
                }
                List<FlatMessage> flatMessages = FlatMessageBuilder.build(database, table, data);
                kafkaMsgProducer.send(canalDestination, flatMessages);
                return true;
            }  catch (Exception e) {
                logger.error(e.getMessage(), e);
                errMsg.add(kafkaMapping.getTopic() + " etl failed! ==>" + e.getMessage());
                throw new RuntimeException(e);
            }
        });
        return false;
    }
}
