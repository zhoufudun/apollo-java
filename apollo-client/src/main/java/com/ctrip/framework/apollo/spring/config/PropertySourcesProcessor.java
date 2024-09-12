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

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.spring.events.ApolloConfigChangeEvent;
import com.ctrip.framework.apollo.spring.util.PropertySourcesUtil;
import com.ctrip.framework.apollo.spring.util.SpringInjector;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.env.*;

/**
 * Apollo Property Sources processor for Spring Annotation Based Application. <br /> <br />
 * <p>
 * The reason why PropertySourcesProcessor implements {@link BeanFactoryPostProcessor} instead of
 * {@link org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor} is that lower versions of
 * Spring (e.g. 3.1.1) doesn't support registering BeanDefinitionRegistryPostProcessor in ImportBeanDefinitionRegistrar
 * - {@link com.ctrip.framework.apollo.spring.annotation.ApolloConfigRegistrar}
 * <p>
 * https://blog.csdn.net/weixin_45505313/article/details/117994726
 * <p>
 * 这个类的核心方法是postProcessBeanFactory，它在Spring容器初始化后调用。在这个方法中，它首先初始化Apollo的配置工具ConfigUtil，然后调用initializePropertySources方法来加载Apollo的配置属性源，并将它们添加到Spring的环境中。如果配置了自动更新属性的功能，它还会调用initializeAutoUpdatePropertiesFeature方法来注册一个监听器，当Apollo配置更改时，会发布一个事件通知。
 * initializePropertySources方法首先检查environment中是否已经包含了Apollo的属性源，如果已经存在，则直接返回。否则，它会创建一个新的CompositePropertySource，并按照order对NAMESPACE_NAMES中的配置名称进行排序，然后逐个加载配置，并将它们添加到CompositePropertySource中。最后，它会将这个CompositePropertySource添加到environment的属性源中。
 * initializeAutoUpdatePropertiesFeature方法则是为了支持Apollo配置的自动更新。它会遍历所有的配置属性源，并为每个属性源添加一个更改监听器。当配置更改时，监听器会发布一个ApolloConfigChangeEvent事件。
 * 此外，该类还实现了setEnvironment、getOrder和setApplicationEventPublisher方法，分别用于设置Spring的环境、获取处理的优先级和设置事件发布器。
 * 总的来说，这个类是Apollo配置与Spring框架集成的关键组件，它确保了Apollo的配置能够被Spring应用正确加载和使用
 * <p>
 * <p>
 * PropertySourcesProcessor 是 Apollo 最关键的组件之一，并且其实例化优先级也是最高的，PropertySourcesProcessor#postProcessBeanFactory()
 * 会在该类实例化的时候被回调，该方法的处理如下：
 * <p>
 * BeanFactoryPostProcessor BeanDefinition定义加载到容器，实例化之前调用
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class PropertySourcesProcessor implements BeanFactoryPostProcessor, EnvironmentAware,
        ApplicationEventPublisherAware, PriorityOrdered {
    // key=order, value=namespaces
    private static final Multimap<Integer, String> NAMESPACE_NAMES = LinkedHashMultimap.create();
    private static final Set<BeanFactory> AUTO_UPDATE_INITIALIZED_BEAN_FACTORIES = Sets.newConcurrentHashSet();

    private final ConfigPropertySourceFactory configPropertySourceFactory = SpringInjector
            .getInstance(ConfigPropertySourceFactory.class);
    private ConfigUtil configUtil;
    private ConfigurableEnvironment environment;
    private ApplicationEventPublisher applicationEventPublisher;

    public static boolean addNamespaces(Collection<String> namespaces, int order) {
        return NAMESPACE_NAMES.putAll(order, namespaces);
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        this.configUtil = ApolloInjector.getInstance(ConfigUtil.class);
        // 调用 PropertySourcesProcessor#initializePropertySources() 拉取远程 namespace 配置
        initializePropertySources();
        // 调用 PropertySourcesProcessor#initializeAutoUpdatePropertiesFeature() 给所有缓存在本地的 Config 配置添加监听器
        initializeAutoUpdatePropertiesFeature(beanFactory);
    }

    /**
     * 从apollo服务端拉取配置封装成confg对象加入到environment中
     * https://blog.51cto.com/u_16099278/8869265
     * https://blog.51cto.com/u_14691/11376981
     * https://blog.csdn.net/lh87270202/article/details/115940023
     */
    private void initializePropertySources() {
        // 检查是否已经初始化过
        if (environment.getPropertySources().contains(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME)) {
            //already initialized
            return;
        }
        CompositePropertySource composite;
        // 如果开启了缓存，则使用 CachedCompositePropertySource
        if (configUtil.isPropertyNamesCacheEnabled()) {
            composite = new CachedCompositePropertySource(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME);
        } else {
            // 创建一个组合属性源CompositePropertySource，属性源名称是ApolloPropertySources，用于存储所有的ConfigPropertySource
            composite = new CompositePropertySource(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME);
        }

        // 按照order升序排序
        ImmutableSortedSet<Integer> orders = ImmutableSortedSet.copyOf(NAMESPACE_NAMES.keySet());
        Iterator<Integer> iterator = orders.iterator();

        // 遍历所有的namespace，将其添加到 CompositePropertySource 中
        while (iterator.hasNext()) {
            int order = iterator.next();
            for (String namespace : NAMESPACE_NAMES.get(order)) {
                // 获取命名空间的配置对象--》从远程服务获取
                Config config = ConfigService.getConfig(namespace);
                // 每个namespace对应一个ConfigPropertySource加入composite中
                composite.addPropertySource(configPropertySourceFactory.getConfigPropertySource(namespace, config));
            }
        }

        // clean up
        NAMESPACE_NAMES.clear();

        // add after the bootstrap property source or to the first
        if (environment.getPropertySources().contains(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME)) {
            // 检查是否开启了覆盖系统属性的功能
            if (configUtil.isOverrideSystemProperties()) {
                // ensure ApolloBootstrapPropertySources is still the first
                // 确保ApolloBootstrapPropertySources在最前面，因为apollo的配置是最优先的
                PropertySourcesUtil.ensureBootstrapPropertyPrecedence(environment);
            }
            // composite添加到第二个位置，apollo的配置优先其他配置
            environment.getPropertySources().addAfter(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME, composite);
        } else {
            // APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME_NAME不存在
            // 没有开启覆盖系统属性的功能
            if (!configUtil.isOverrideSystemProperties()) {
                // 检查是否存在系统环境变量的属性源，如果存在，则将composite添加到其后面
                if (environment.getPropertySources().contains(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME)) {
                    // apollo的配置在系统环境变量后面，系统环境变量的优先级比apollo的配置高
                    environment.getPropertySources().addAfter(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, composite);
                    return;
                }
            }
            // 添加到第一个位置，apollo的配置优先其他配置
            environment.getPropertySources().addFirst(composite);
        }
    }

    // com.ctrip.framework.apollo.spring.config.PropertySourcesProcessor#initializeAutoUpdatePropertiesFeature
    private void initializeAutoUpdatePropertiesFeature(ConfigurableListableBeanFactory beanFactory) {
        // 检查是否已经初始化过
        if (!AUTO_UPDATE_INITIALIZED_BEAN_FACTORIES.add(beanFactory)) {
            return;
        }

        // 当收到配置变更回调后，会发送 ApolloConfigChangeEvent 事件
        ConfigChangeListener configChangeEventPublisher = changeEvent ->
                // 使用spring的事件发布器发布事件：ApolloConfigChangeEvent
                // 发布事件后会被AutoUpdateConfigChangeListener监听到调用AutoUpdateConfigChangeListener#onChange
                applicationEventPublisher.publishEvent(new ApolloConfigChangeEvent(changeEvent));

        List<ConfigPropertySource> configPropertySources = configPropertySourceFactory.getAllConfigPropertySources();
        // 遍历所有的 ConfigPropertySource， 将监听器注册到ConfigPropertySource【一个namespace对应一个ConfigPropertySource，对应一个DefaultConfig】 中
        // 后续每个namespace下有变动时，会触发监听器，spring的事件发布器发布ApolloConfigChangeEvent事件通知
        for (ConfigPropertySource configPropertySource : configPropertySources) {
            // 将配置变更监听器注册进 DefaultConfig中
            configPropertySource.addChangeListener(configChangeEventPublisher);
        }
    }


    @Override
    public void setEnvironment(Environment environment) {
        //it is safe enough to cast as all known environment is derived from ConfigurableEnvironment
        this.environment = (ConfigurableEnvironment) environment;
    }

    @Override
    public int getOrder() {
        //make it as early as possible
        return Ordered.HIGHEST_PRECEDENCE;
    }

    // for test only
    static void reset() {
        NAMESPACE_NAMES.clear();
        AUTO_UPDATE_INITIALIZED_BEAN_FACTORIES.clear();
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }
}
