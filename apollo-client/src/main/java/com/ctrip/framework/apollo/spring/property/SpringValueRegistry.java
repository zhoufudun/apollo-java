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
package com.ctrip.framework.apollo.spring.property;

import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;

/**
 * 这个类的主要功能是注册和管理Spring框架中的值（SpringValue），这些值通常与 Apollo 配置管理系统集成，用于动态获取和更新配置属性
 * 主要封装属性中含有占位符${key}中的key找出来，key和field等做好关联，放入map集合中，方便后续拉取最新配置后实时找到对应映射关系并反射设置新值
 * https://blog.51cto.com/u_14691/11376981
 *
 * 这个类的作用参考：https://blog.51cto.com/u_14691/11376981
 */
public class SpringValueRegistry {
    private static final Logger logger = LoggerFactory.getLogger(SpringValueRegistry.class);
    private static final long CLEAN_INTERVAL_IN_SECONDS = 5;

    // Multimap是一个key对应多个实体value的map集合，存放过程中自动将实体进行归
    private final Map<BeanFactory, Multimap<String, SpringValue>> registry = Maps.newConcurrentMap();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Object LOCK = new Object();

    public void register(BeanFactory beanFactory, String key, SpringValue springValue) {
        if (!registry.containsKey(beanFactory)) {
            synchronized (LOCK) {
                if (!registry.containsKey(beanFactory)) {
                    registry.put(beanFactory, Multimaps.synchronizedListMultimap(LinkedListMultimap.create()));
                }
            }
        }

        registry.get(beanFactory).put(key, springValue);

        // lazy initialize
        if (initialized.compareAndSet(false, true)) {
            initialize();
        }
    }

    public Collection<SpringValue> get(BeanFactory beanFactory, String key) {
        Multimap<String, SpringValue> beanFactorySpringValues = registry.get(beanFactory);
        if (beanFactorySpringValues == null) {
            return null;
        }
        return beanFactorySpringValues.get(key);
    }

    /**
     * 使用 ScheduledExecutorService 定期执行 scanAndClean 方法，清理无效的 SpringValue 对象，保持注册表的整洁和高效
     *
     */
    private void initialize() {
        Executors.newSingleThreadScheduledExecutor(ApolloThreadFactory.create("SpringValueRegistry", true)).scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            scanAndClean();
                        } catch (Throwable ex) {
                            logger.error(ex.getMessage(), ex);
                        }
                    }
                }, CLEAN_INTERVAL_IN_SECONDS, CLEAN_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * scanAndClean 方法遍历注册表中的所有 BeanFactory，检查每个 SpringValue 对象的目标 bean 是否仍然有效。
     * 如果目标 bean 无效，则从注册表中移除相应的 SpringValue 对象，以释放资源
     */
    private void scanAndClean() {
        Iterator<Multimap<String, SpringValue>> iterator = registry.values().iterator();
        while (!Thread.currentThread().isInterrupted() && iterator.hasNext()) {
            Multimap<String, SpringValue> springValues = iterator.next();
            Iterator<Entry<String, SpringValue>> springValueIterator = springValues.entries().iterator();
            while (springValueIterator.hasNext()) {
                Entry<String, SpringValue> springValue = springValueIterator.next();
                if (!springValue.getValue().isTargetBeanValid()) {
                    // clear unused spring values
                    springValueIterator.remove();
                }
            }
        }
    }
}
