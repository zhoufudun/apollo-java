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
 *
 * https://blog.csdn.net/weixin_45505313/article/details/117994726
 *
 * 这个类的核心方法是postProcessBeanFactory，它在Spring容器初始化后调用。在这个方法中，它首先初始化Apollo的配置工具ConfigUtil，然后调用initializePropertySources方法来加载Apollo的配置属性源，并将它们添加到Spring的环境中。如果配置了自动更新属性的功能，它还会调用initializeAutoUpdatePropertiesFeature方法来注册一个监听器，当Apollo配置更改时，会发布一个事件通知。
 * initializePropertySources方法首先检查environment中是否已经包含了Apollo的属性源，如果已经存在，则直接返回。否则，它会创建一个新的CompositePropertySource，并按照order对NAMESPACE_NAMES中的配置名称进行排序，然后逐个加载配置，并将它们添加到CompositePropertySource中。最后，它会将这个CompositePropertySource添加到environment的属性源中。
 * initializeAutoUpdatePropertiesFeature方法则是为了支持Apollo配置的自动更新。它会遍历所有的配置属性源，并为每个属性源添加一个更改监听器。当配置更改时，监听器会发布一个ApolloConfigChangeEvent事件。
 * 此外，该类还实现了setEnvironment、getOrder和setApplicationEventPublisher方法，分别用于设置Spring的环境、获取处理的优先级和设置事件发布器。
 * 总的来说，这个类是Apollo配置与Spring框架集成的关键组件，它确保了Apollo的配置能够被Spring应用正确加载和使用
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class PropertySourcesProcessor implements BeanFactoryPostProcessor, EnvironmentAware,
        ApplicationEventPublisherAware, PriorityOrdered {
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
        initializePropertySources();
        initializeAutoUpdatePropertiesFeature(beanFactory);
    }

    private void initializePropertySources() {
        if (environment.getPropertySources().contains(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME)) {
            //already initialized
            return;
        }
        CompositePropertySource composite;
        if (configUtil.isPropertyNamesCacheEnabled()) {
            composite = new CachedCompositePropertySource(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME);
        } else {
            composite = new CompositePropertySource(PropertySourcesConstants.APOLLO_PROPERTY_SOURCE_NAME);
        }

        //sort by order asc
        ImmutableSortedSet<Integer> orders = ImmutableSortedSet.copyOf(NAMESPACE_NAMES.keySet());
        Iterator<Integer> iterator = orders.iterator();

        while (iterator.hasNext()) {
            int order = iterator.next();
            for (String namespace : NAMESPACE_NAMES.get(order)) {
                Config config = ConfigService.getConfig(namespace);

                composite.addPropertySource(configPropertySourceFactory.getConfigPropertySource(namespace, config));
            }
        }

        // clean up
        NAMESPACE_NAMES.clear();

        // add after the bootstrap property source or to the first
        if (environment.getPropertySources()
                .contains(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME)) {

            if (configUtil.isOverrideSystemProperties()) {
                // ensure ApolloBootstrapPropertySources is still the first
                PropertySourcesUtil.ensureBootstrapPropertyPrecedence(environment);
            }

            environment.getPropertySources()
                    .addAfter(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME, composite);
        } else {
            if (!configUtil.isOverrideSystemProperties()) {
                if (environment.getPropertySources().contains(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME)) {
                    environment.getPropertySources().addAfter(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, composite);
                    return;
                }
            }
            environment.getPropertySources().addFirst(composite);
        }
    }

    private void initializeAutoUpdatePropertiesFeature(ConfigurableListableBeanFactory beanFactory) {
        if (!AUTO_UPDATE_INITIALIZED_BEAN_FACTORIES.add(beanFactory)) {
            return;
        }

        ConfigChangeListener configChangeEventPublisher = changeEvent ->
                applicationEventPublisher.publishEvent(new ApolloConfigChangeEvent(changeEvent));

        List<ConfigPropertySource> configPropertySources = configPropertySourceFactory.getAllConfigPropertySources();
        for (ConfigPropertySource configPropertySource : configPropertySources) {
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
