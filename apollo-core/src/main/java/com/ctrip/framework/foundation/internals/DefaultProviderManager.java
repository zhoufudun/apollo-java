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
package com.ctrip.framework.foundation.internals;

import java.util.LinkedHashMap;
import java.util.Map;

import com.ctrip.framework.foundation.internals.provider.DefaultApplicationProvider;
import com.ctrip.framework.foundation.internals.provider.DefaultNetworkProvider;
import com.ctrip.framework.foundation.internals.provider.DefaultServerProvider;
import com.ctrip.framework.foundation.spi.ProviderManager;
import com.ctrip.framework.foundation.spi.provider.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProviderManager implements ProviderManager {
    private static final Logger logger = LoggerFactory.getLogger(DefaultProviderManager.class);
    /**
     * {interface com.ctrip.framework.foundation.spi.provider.ApplicationProvider=appId [fix-server] properties: {} (DefaultApplicationProvider),
     * interface com.ctrip.framework.foundation.spi.provider.NetworkProvider=hostName [WIN-20230608VMY] hostIP [192.168.254.1] (DefaultNetworkProvider),
     * interface com.ctrip.framework.foundation.spi.provider.ServerProvider=environment [null] data center [null] properties: {} (DefaultServerProvider)}
     */
    private Map<Class<? extends Provider>, Provider> m_providers = new LinkedHashMap<>();

    /**
     * 从 3 个不同的 Provider 中获取配置
     */
    @Override
    public void initialize() {
        // 从//META-INF/app.properties 中获取配置
        // Load per-application configuration, like app id, from classpath://META-INF/app.properties
        Provider applicationProvider = new DefaultApplicationProvider();
        applicationProvider.initialize();
        register(applicationProvider);

        // Load network parameters
        // 从网络中获取配置
        Provider networkProvider = new DefaultNetworkProvider();
        networkProvider.initialize();
        register(networkProvider);

        // Load environment (fat, fws, uat, prod ...) and dc, from /opt/settings/server.properties, JVM property and/or OS
        // environment variables.
        // 从文件中获取配置，例如环境信息
        Provider serverProvider = new DefaultServerProvider();
        serverProvider.initialize();
        register(serverProvider);
    }

    public synchronized void register(Provider provider) {
        m_providers.put(provider.getType(), provider);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Provider> T provider(Class<T> clazz) { // interface com.ctrip.framework.foundation.spi.provider.ServerProvider
        Provider provider = m_providers.get(clazz);

        if (provider != null) {
            return (T) provider;
        }
        logger.error("No provider [{}] found in DefaultProviderManager, please make sure it is registered in DefaultProviderManager ",
                clazz.getName());
        return (T) NullProviderManager.provider;
    }

    @Override
    public String getProperty(String name, String defaultValue) {
        for (Provider provider : m_providers.values()) {
            String value = provider.getProperty(name, null);

            if (value != null) {
                return value;
            }
        }

        return defaultValue;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(512);
        if (null != m_providers) {
            for (Map.Entry<Class<? extends Provider>, Provider> entry : m_providers.entrySet()) {
                sb.append(entry.getValue()).append("\n");
            }
        }
        sb.append("(DefaultProviderManager)").append("\n");
        return sb.toString();
    }
}
