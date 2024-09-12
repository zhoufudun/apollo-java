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

import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.spi.ApolloInjectorCustomizer;
import com.ctrip.framework.apollo.spi.ConfigFactory;
import com.ctrip.framework.apollo.spi.ConfigFactoryManager;
import com.ctrip.framework.apollo.spi.ConfigRegistry;
import com.ctrip.framework.apollo.spi.DefaultConfigFactory;
import com.ctrip.framework.apollo.spi.DefaultConfigFactoryManager;
import com.ctrip.framework.apollo.spi.DefaultConfigRegistry;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.factory.DefaultPropertiesFactory;
import com.ctrip.framework.apollo.util.factory.PropertiesFactory;
import com.ctrip.framework.apollo.util.http.DefaultHttpClient;
import com.ctrip.framework.apollo.util.http.HttpClient;

import com.ctrip.framework.apollo.util.yaml.YamlParser;
import com.ctrip.framework.foundation.internals.ServiceBootstrap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Singleton;

import java.util.List;

/**
 * Guice injector
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultInjector implements Injector {
    private final com.google.inject.Injector m_injector;
    private final List<ApolloInjectorCustomizer> m_customizers;

    public DefaultInjector() {
        try {
            m_injector = Guice.createInjector(new ApolloModule());
            m_customizers = ServiceBootstrap.loadAllOrdered(ApolloInjectorCustomizer.class);
        } catch (Throwable ex) {
            ApolloConfigException exception = new ApolloConfigException("Unable to initialize Guice Injector!", ex);
            Tracer.logError(exception);
            throw exception;
        }
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        try {
            for (ApolloInjectorCustomizer customizer : m_customizers) {
                T instance = customizer.getInstance(clazz);
                if (instance != null) {
                    return instance;
                }
            }
            return m_injector.getInstance(clazz);
        } catch (Throwable ex) {
            Tracer.logError(ex);
            throw new ApolloConfigException(
                    String.format("Unable to load instance for %s!", clazz.getName()), ex);
        }
    }

    @Override
    public <T> T getInstance(Class<T> clazz, String name) {
        try {
            for (ApolloInjectorCustomizer customizer : m_customizers) {
                T instance = customizer.getInstance(clazz, name);
                if (instance != null) {
                    return instance;
                }
            }
            //Guice does not support get instance by type and name
            return null;
        } catch (Throwable ex) {
            Tracer.logError(ex);
            throw new ApolloConfigException(
                    String.format("Unable to load instance for %s with name %s!", clazz.getName(), name), ex);
        }
    }
    // ApolloModule 类的 configure 方法是为了在 Spring 应用程序启动时配置依赖项，使得 Apollo 能够与 Guice 一起工作，提供应用运行所需的各种配置和服务，同时确保这些组件以单例方式提供，避免资源浪费和不一致状态。
    private static class ApolloModule extends AbstractModule {
        @Override
        protected void configure() {
            // 这些绑定指定了在需要相应类型的实例时，Guice 应该提供哪个具体类的实例
            bind(ConfigManager.class).to(DefaultConfigManager.class).in(Singleton.class);
            bind(ConfigFactoryManager.class).to(DefaultConfigFactoryManager.class).in(Singleton.class);
            bind(ConfigRegistry.class).to(DefaultConfigRegistry.class).in(Singleton.class);
            bind(ConfigFactory.class).to(DefaultConfigFactory.class).in(Singleton.class);
            bind(ConfigUtil.class).in(Singleton.class);
            bind(HttpClient.class).to(DefaultHttpClient.class).in(Singleton.class);
            bind(ConfigServiceLocator.class).in(Singleton.class);
            bind(RemoteConfigLongPollService.class).in(Singleton.class);
            bind(YamlParser.class).in(Singleton.class);
            bind(PropertiesFactory.class).to(DefaultPropertiesFactory.class).in(Singleton.class);
        }
    }
}
