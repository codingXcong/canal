package com.alibaba.otter.canal.client.adapter.kafka.config;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka表映射配置加载器
 * @author codingxcong
 * @date 2022-09-29
 */
public class KafkaSyncConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(KafkaSyncConfigLoader.class);

    /**
     * 加载kafka映射配置
     *
     * @return 配置名/配置文件名--对象
     */
    public static Map<String, KafkaSyncConfig> load(Properties envProperties) {
        logger.info("## Start loading hbase mapping config ... ");

        Map<String, KafkaSyncConfig> result = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("kafka");
        configContentMap.forEach((fileName, content) -> {
            KafkaSyncConfig config = YmlConfigBinder
                    .bindYmlToObj(null, content, KafkaSyncConfig.class, null, envProperties);
            if (config == null) {
                return;
            }
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR load Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });

        logger.info("## Hbase mapping config loaded");
        return result;
    }

}
