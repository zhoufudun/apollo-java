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
package com.ctrip.framework.apollo.spring.config;

import java.util.List;

import com.ctrip.framework.apollo.Config;
import com.google.common.collect.Lists;

public class ConfigPropertySourceFactory {

    // 用于缓存所有的 ConfigPropertySource 对象，一个namespace对应一个ConfigPropertySource，一个ConfigPropertySource 对应一个Config
    private final List<ConfigPropertySource> configPropertySources = Lists.newLinkedList();

    // 每个namespace对应一个ConfigPropertySource
    public ConfigPropertySource getConfigPropertySource(String name, Config source) {
        // 将 Apollo 的 Config 配置封装为继承自 Spring 内置的 EnumerablePropertySource 类的 ConfigPropertySource 对象
        ConfigPropertySource configPropertySource = new ConfigPropertySource(name, source);

        // 将新生成的 ConfigPropertySource 对象缓存到内部列表，以备后续为每个配置实例添加配置变化监听器使用
        configPropertySources.add(configPropertySource);

        return configPropertySource;
    }

    public List<ConfigPropertySource> getAllConfigPropertySources() {
        return Lists.newLinkedList(configPropertySources);
    }
}
