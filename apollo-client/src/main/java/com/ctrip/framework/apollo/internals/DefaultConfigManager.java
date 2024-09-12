/*
 * Copyright 2022 Apollo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ctrip.framework.apollo.internals;

import java.util.Map;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.spi.ConfigFactory;
import com.ctrip.framework.apollo.spi.ConfigFactoryManager;
import com.google.common.collect.Maps;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultConfigManager implements ConfigManager {
    private ConfigFactoryManager m_factoryManager;

    private Map<String, Config> m_configs = Maps.newConcurrentMap();
    private Map<String, Object> m_configLocks = Maps.newConcurrentMap();
    private Map<String, ConfigFile> m_configFiles = Maps.newConcurrentMap();
    private Map<String, Object> m_configFileLocks = Maps.newConcurrentMap();

    public DefaultConfigManager() {
        m_factoryManager = ApolloInjector.getInstance(ConfigFactoryManager.class);
    }

    /**
     * 首先从缓存中获取配置，缓存中没有则从远程拉取，注意此处在 synchronized 代码块内部也判了一次空，采用了双重检查锁机制。
     * 远程拉取配置首先需要通过 ConfigFactoryManager#getFactory() 方法获取 ConfigFactory 实例，这里实际上获取的是DefaultConfigFactory，
     * 再通过 DefaultConfigFactory#create() 去获取 Apollo Server 中的配置。
     * @param namespace the namespace
     * @return
     */
    @Override
    public Config getConfig(String namespace) {
        // 首先从缓存中获取配置，缓存中没有则从远程拉取，注意此处在 synchronized 代码块内部也判了一次空，采用了双重检查锁机制
        Config config = m_configs.get(namespace);

        if (config == null) {
            synchronized (this) {
                config = m_configs.get(namespace);
                // 加锁后再次判断
                if (config == null) {
                    // 远程拉取配置首先需要通过 ConfigFactoryManager#getFactory() 方法获取 ConfigFactory 实例
                    ConfigFactory factory = m_factoryManager.getFactory(namespace);
                    // 再通过 ConfigFactory#create() 去实际地进行拉取操作。此处 Factory 的创建也使用了 ServiceLoader 机制，暂不讨论，可知最后实际调用到 DefaultConfigFactory#create()
                    config = factory.create(namespace);
                    // 将从远端拉取到的配置缓存
                    m_configs.put(namespace, config);
                }
            }
        }

        return config;
    }

    @Override
    public ConfigFile getConfigFile(String namespace, ConfigFileFormat configFileFormat) {
        String namespaceFileName = String.format("%s.%s", namespace, configFileFormat.getValue());
        ConfigFile configFile = m_configFiles.get(namespaceFileName);

        if (configFile == null) {
            Object lock = m_configFileLocks.computeIfAbsent(namespaceFileName, key -> new Object());
            synchronized (lock) {
                configFile = m_configFiles.get(namespaceFileName);

                if (configFile == null) {
                    ConfigFactory factory = m_factoryManager.getFactory(namespaceFileName);

                    configFile = factory.createConfigFile(namespaceFileName, configFileFormat);
                    m_configFiles.put(namespaceFileName, configFile);
                }
            }
        }

        return configFile;
    }
}
