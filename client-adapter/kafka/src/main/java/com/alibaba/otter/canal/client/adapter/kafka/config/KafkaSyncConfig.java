package com.alibaba.otter.canal.client.adapter.kafka.config;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

/**
 * @author codingxcong
 * @date 2022-09-28
 */
public class KafkaSyncConfig implements AdapterConfig {

    /** 源数据库url */
    private String dataSourceKey;

    private KafkaMapping kafkaMapping;

    // adapter key
    private String  outerAdapterKey;

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public KafkaMapping getKafkaMapping() {
        return kafkaMapping;
    }

    public void setKafkaMapping(KafkaMapping kafkaMapping) {
        this.kafkaMapping = kafkaMapping;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public void validate() {

    }

    public static class KafkaMapping implements AdapterMapping {

        /** 需要回溯的sql */
        private String sql;

        /** 要发往kafka的topic */
        private String topic;

        /** 分区 */
        private Integer partition;

        /** 数据库scheme，方便构建MQ消息内容 */
        private String database;

        /** 表名，方便构建MQ消息内容 */
        private String table;

        private String etlCondition;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        @Override
        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }
    }


    @Override
    public String getDataSourceKey() {
        return dataSourceKey;
    }

    @Override
    public KafkaMapping getMapping() {
        return kafkaMapping;
    }


}
