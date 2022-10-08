package com.alibaba.otter.canal.client.adapter.kafka;

import com.alibaba.otter.canal.adapter.launcher.config.SpringContext;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.kafka.config.KafkaSyncConfig;
import com.alibaba.otter.canal.client.adapter.kafka.config.KafkaSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.kafka.etl.KafkaEtlService;
import com.alibaba.otter.canal.client.adapter.kafka.support.KafkaMsgProducer;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * kafka 外部适配器，主要用于 mysql -> kafka的历史数据回溯
 * @author codingxcong
 * @date 2022-09-28
 */
@SPI("kafka")
public class KafkaAdapter implements OuterAdapter {

    private Properties envProperties;
    private KafkaMsgProducer kafkaMQProducer;
    /** 文件名对应配置 */
    private Map<String, KafkaSyncConfig> kafkaSyncConfig = new ConcurrentHashMap<>();

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            this.envProperties = envProperties;
            kafkaSyncConfig = KafkaSyncConfigLoader.load(envProperties);
            String kafkaServers = envProperties.getProperty("canal.conf.consumerProperties.kafka.bootstrap.servers");
            kafkaMQProducer = new KafkaMsgProducer(kafkaServers);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        // 主要用于历史数据回溯，增量数据啥都不干
    }

    @Override
    public void destroy() {
        if (kafkaMQProducer != null) {
            kafkaMQProducer.stop();
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        if (kafkaMQProducer == null) {
            Environment env = (Environment) SpringContext.getBean(Environment.class);
            Properties evnProperties = null;
            if (env instanceof StandardEnvironment) {
                evnProperties = new Properties();
                for (PropertySource<?> propertySource : ((StandardEnvironment) env).getPropertySources()) {
                    if (propertySource instanceof EnumerablePropertySource) {
                        String[] names = ((EnumerablePropertySource<?>) propertySource).getPropertyNames();
                        for (String name : names) {
                            Object val = env.getProperty(name);
                            if (val != null) {
                                evnProperties.put(name, val);
                            }
                        }
                    }
                }
            }

            this.init(null, evnProperties);
        }

        EtlResult etlResult = new EtlResult();
        KafkaSyncConfig config = kafkaSyncConfig.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            KafkaEtlService kafkaEtlService = new KafkaEtlService(kafkaMQProducer, config);
            if (dataSource != null) {
                return kafkaEtlService.importData(params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        }
        return etlResult;
    }

    @Override
    public Map<String, Object> count(String task) {
        return OuterAdapter.super.count(task);
    }

    @Override
    public String getDestination(String task) {
        return OuterAdapter.super.getDestination(task);
    }
}
