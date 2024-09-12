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
package com.ctrip.framework.apollo.spi;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.PropertiesCompatibleConfigFile;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.internals.ConfigRepository;
import com.ctrip.framework.apollo.internals.DefaultConfig;
import com.ctrip.framework.apollo.internals.JsonConfigFile;
import com.ctrip.framework.apollo.internals.LocalFileConfigRepository;
import com.ctrip.framework.apollo.internals.PropertiesCompatibleFileConfigRepository;
import com.ctrip.framework.apollo.internals.PropertiesConfigFile;
import com.ctrip.framework.apollo.internals.RemoteConfigRepository;
import com.ctrip.framework.apollo.internals.TxtConfigFile;
import com.ctrip.framework.apollo.internals.XmlConfigFile;
import com.ctrip.framework.apollo.internals.YamlConfigFile;
import com.ctrip.framework.apollo.internals.YmlConfigFile;
import com.ctrip.framework.apollo.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default implementation of {@link ConfigFactory}.
 * <p>
 * Supports namespaces of format:
 * <ul>
 *   <li>{@link ConfigFileFormat#Properties}</li>
 *   <li>{@link ConfigFileFormat#XML}</li>
 *   <li>{@link ConfigFileFormat#JSON}</li>
 *   <li>{@link ConfigFileFormat#YML}</li>
 *   <li>{@link ConfigFileFormat#YAML}</li>
 *   <li>{@link ConfigFileFormat#TXT}</li>
 * </ul>
 *
 * @author Jason Song(song_s@ctrip.com)
 * @author Diego Krupitza(info@diegokrupitza.com)
 */
public class DefaultConfigFactory implements ConfigFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultConfigFactory.class);
    private final ConfigUtil m_configUtil;

    public DefaultConfigFactory() {
        m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    }

    //com.ctrip.framework.apollo.spi.DefaultConfigFactory#create

    /**
     * Apollo 通过多层 ConfigRepository 设计实现如下配置加载机制，既保证了配置的实时性，又保证了Apollo Server出现故障时对接入的服务影响最小：
     * 1、客户端和服务端保持了一个长连接（通过Http Long Polling实现），从而能第一时间获得配置更新的推送（RemoteConfigRepository）
     * 2、客户端还会定时从Apollo配置中心服务端拉取应用的最新配置。这是一个fallback机制，为了防止推送机制失效导致配置不更新。客户端定时拉取会上报本地版本，所以一般情况下，对于定时拉取的操作，服务端都会返回304 - Not Modified
     * 定时频率默认为每5分钟拉取一次，客户端也可以通过在运行时指定System Property：apollo.refreshInterval来覆盖，单位为分钟
     * 客户端会把从服务端获取到的配置在本地文件系统缓存一份在遇到服务不可用，或网络不通的时候，依然能从本地恢复配置（LocalFileConfigRepository）
     * 客户端从Apollo配置中心服务端获取到应用的最新配置后，会保存在内存中（DefaultConfig）
     * @param namespace the namespace
     * @return
     */
    @Override
    public Config create(String namespace) {
        // 确定本地配置缓存文件的格式。对于格式不是属性的命名空间，必须提供文件扩展名，例如application.yaml
        ConfigFileFormat format = determineFileFormat(namespace);

        ConfigRepository configRepository = null;

        if (ConfigFileFormat.isPropertiesCompatible(format) &&
                format != ConfigFileFormat.Properties) {
            // 如果是YML类型的配置
            configRepository = createPropertiesCompatibleFileConfigRepository(namespace, format);
        } else {
            // 如果是 Properties 类型的配置
            configRepository = createConfigRepository(namespace);
        }

        logger.debug("Created a configuration repository of type [{}] for namespace [{}]",
                configRepository.getClass().getName(), namespace);

        // 创建 DefaultConfig对象，并将当前 DefaultConfig 对象 对象注册进 configRepository 更新通知列表，这样configRepository中的配置发生变更时，就会通知 DefaultConfig
        return this.createRepositoryConfig(namespace, configRepository);
    }

    protected Config createRepositoryConfig(String namespace, ConfigRepository configRepository) {
        return new DefaultConfig(namespace, configRepository);
    }

    @Override
    public ConfigFile createConfigFile(String namespace, ConfigFileFormat configFileFormat) {
        ConfigRepository configRepository = createConfigRepository(namespace);
        switch (configFileFormat) {
            case Properties:
                return new PropertiesConfigFile(namespace, configRepository);
            case XML:
                return new XmlConfigFile(namespace, configRepository);
            case JSON:
                return new JsonConfigFile(namespace, configRepository);
            case YAML:
                return new YamlConfigFile(namespace, configRepository);
            case YML:
                return new YmlConfigFile(namespace, configRepository);
            case TXT:
                return new TxtConfigFile(namespace, configRepository);
        }

        return null;
    }

    /**
     * LocalFileConfigRepository：从本地文件中加载配置。
     * RemoteConfigRepository：从远端Apollo Server加载配置。
     * @param namespace
     * @return
     */
    ConfigRepository createConfigRepository(String namespace) {
        if (m_configUtil.isPropertyFileCacheEnabled()) {
            // 默认是开启缓存机制的
            return createLocalConfigRepository(namespace);
        }
        return createRemoteConfigRepository(namespace);
    }

    /**
     * Creates a local repository for a given namespace
     *
     * @param namespace the namespace of the repository
     * @return the newly created repository for the given namespace
     */
    LocalFileConfigRepository createLocalConfigRepository(String namespace) {
        if (m_configUtil.isInLocalMode()) {
            logger.warn(
                    "==== Apollo is in local mode! Won't pull configs from remote server for namespace {} ! ====",
                    namespace);
            return new LocalFileConfigRepository(namespace);
        }
        // 创建 RemoteConfigRepository 和 LocalFileConfigRepository，并将 LocalFileConfigRepository 注册进 RemoteConfigRepository的变更通知列表中
        return new LocalFileConfigRepository(namespace, createRemoteConfigRepository(namespace));
    }

    RemoteConfigRepository createRemoteConfigRepository(String namespace) {
        return new RemoteConfigRepository(namespace);
    }

    PropertiesCompatibleFileConfigRepository createPropertiesCompatibleFileConfigRepository(
            String namespace, ConfigFileFormat format) {
        String actualNamespaceName = trimNamespaceFormat(namespace, format);
        PropertiesCompatibleConfigFile configFile = (PropertiesCompatibleConfigFile) ConfigService
                .getConfigFile(actualNamespaceName, format);

        return new PropertiesCompatibleFileConfigRepository(configFile);
    }

    // for namespaces whose format are not properties, the file extension must be present, e.g. application.yaml
    ConfigFileFormat determineFileFormat(String namespaceName) {
        String lowerCase = namespaceName.toLowerCase();
        for (ConfigFileFormat format : ConfigFileFormat.values()) {
            if (lowerCase.endsWith("." + format.getValue())) {
                return format;
            }
        }

        return ConfigFileFormat.Properties;
    }

    String trimNamespaceFormat(String namespaceName, ConfigFileFormat format) {
        String extension = "." + format.getValue();
        if (!namespaceName.toLowerCase().endsWith(extension)) {
            return namespaceName;
        }

        return namespaceName.substring(0, namespaceName.length() - extension.length());
    }

}
