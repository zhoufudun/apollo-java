package com.ctrip.framework.apollo.use.cases.spring.boot.apollo.myConfig;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

@Slf4j
public class MyConfig {

    private static final String application = "application";
    // 存储不同命名空间的 Apollo 配置
    private final Map<String, Config> namespaceConfigMap = new LinkedHashMap<>();

    // 初始化时加载指定命名空间的配置
    public MyConfig(String... namespaces) {
        for (String namespace : namespaces) {
            Config config = ConfigService.getConfig(namespace);
            namespaceConfigMap.put(namespace, config);
        }

        namespaceConfigMap.put(application, ConfigService.getConfig(application));

        // 排序namespace优先级最高
        Object[] applications = namespaceConfigMap.keySet().stream().filter(namespace -> {
            // 优先级最高
            return namespace.startsWith(application);
        }).toArray();

        if(applications.length!=1){
            log.error("error todo ");
            // todo
        }else {
            //application移动到最后面
            String applicationNamespace = (String) applications[0];
            Config config = namespaceConfigMap.remove(applicationNamespace);
            namespaceConfigMap.put(applicationNamespace, config);
        }
    }

    // 根据命名空间和key获取配置值
    public String getConfigValue(String namespace, String key, String defaultValue) {
        Config config = namespaceConfigMap.get(namespace);
        if (config != null) {
            return config.getProperty(key, defaultValue);
        }
        return defaultValue;
    }

    // 根据命名空间获取Config
    public Config getConfigByNamespace(String namespace) {
        return namespaceConfigMap.get(namespace);
    }

    // 根据命名空间和key获取int类型的配置值
    public int getIntConfigValue(String namespace, String key, int defaultValue) {
        Config config = namespaceConfigMap.get(namespace);
        if (config != null) {
            return config.getIntProperty(key, defaultValue);
        }
        return defaultValue;
    }

    // 根据命名空间和key获取boolean类型的配置值
    public boolean getBooleanConfigValue(String namespace, String key, boolean defaultValue) {
        Config config = namespaceConfigMap.get(namespace);
        if (config != null) {
            return config.getBooleanProperty(key, defaultValue);
        }
        return defaultValue;
    }

    public Map<String, String> getAllKeyValueMapByNamespace(String namespace) {
        Map<String, String> kvMap = new HashMap<>();
        Config config = namespaceConfigMap.get(namespace);
        if (config!= null) {
            for (String key : config.getPropertyNames()) {
                String value = config.getProperty(key, ""); // 或者根据需要设置默认值
                kvMap.put(key, value);
            }
        }
        return kvMap;
    }

    // 获取所有命名空间的所有键值对
    public Map<String, String> getAllKeyValueMap() {
        Map<String, String> allKvMap = new HashMap<>();
        for (Config config : namespaceConfigMap.values()) {
            if (config!= null) {
                for (String key : config.getPropertyNames()) {
                    String value = config.getProperty(key, ""); // 或者根据需要设置默认值
                    allKvMap.put(key, value);
                }
            }
        }
        return allKvMap;
    }
}