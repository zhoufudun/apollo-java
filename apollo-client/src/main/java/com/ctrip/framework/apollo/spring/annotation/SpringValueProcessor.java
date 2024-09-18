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
package com.ctrip.framework.apollo.spring.annotation;

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.spring.property.PlaceholderHelper;
import com.ctrip.framework.apollo.spring.property.SpringValue;
import com.ctrip.framework.apollo.spring.property.SpringValueDefinition;
import com.ctrip.framework.apollo.spring.property.SpringValueDefinitionProcessor;
import com.ctrip.framework.apollo.spring.property.SpringValueRegistry;
import com.ctrip.framework.apollo.spring.util.SpringInjector;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.Bean;

/**
 * Spring value processor of field or method which has @Value and xml config placeholders.
 *
 * @author github.com/zhegexiaohuozi  seimimaster@gmail.com  mghio.dev@gmail.com
 * @since 2017/12/20.
 */
public class SpringValueProcessor extends ApolloProcessor implements BeanFactoryPostProcessor, BeanFactoryAware {

    private static final Logger logger = LoggerFactory.getLogger(SpringValueProcessor.class);

    private final ConfigUtil configUtil;
    private final PlaceholderHelper placeholderHelper;
    private final SpringValueRegistry springValueRegistry;

    private BeanFactory beanFactory;
    private Multimap<String, SpringValueDefinition> beanName2SpringValueDefinitions; // key=BeanDefinitionRegistry， value=List<SpringValueDefinition>

    public SpringValueProcessor() {
        configUtil = ApolloInjector.getInstance(ConfigUtil.class);
        placeholderHelper = SpringInjector.getInstance(PlaceholderHelper.class);
        springValueRegistry = SpringInjector.getInstance(SpringValueRegistry.class);
        beanName2SpringValueDefinitions = LinkedListMultimap.create();
    }
    /**
     * postProcessBeanFactory 方法会在 postProcessBeforeInitialization 方法之前执行。
     * 在 Spring 框架中，BeanFactoryPostProcessor 接口的 postProcessBeanFactory 方法是在 Spring 容器加载了所有的 Bean 定义之后，但在 Bean 实例化之前被调用的。这个方法主要用于修改 Bean 的定义，例如修改 Bean 的属性值、添加或者移除 Bean 等。
     * 而 postProcessBeforeInitialization 方法是在 BeanPostProcessor 接口中定义的，它在 Spring 容器实例化每个 Bean 之后，在调用 Bean 的初始化方法（如 afterPropertiesSet 或自定义的初始化方法）之前被调用。这个方法主要用于在 Bean 初始化之前对其进行一些额外的处理，例如检查或修改 Bean 的属性值等。
     * 在你的代码中，SpringValueProcessor 类实现了 BeanFactoryPostProcessor 和 BeanPostProcessor 接口，因此它的 postProcessBeanFactory 方法会在所有 Bean 定义被加载后立即执行，而 postProcessBeforeInitialization 方法会在每个 Bean 实例化之后执行。
     * 总结来说，postProcessBeanFactory 方法先执行，因为它是在 Bean 定义加载后但在 Bean 实例化前调用的；而 postProcessBeforeInitialization 方法后执行，因为它是在 Bean 实例化后但在初始化前调用的
     */
    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory)
            throws BeansException {
        if (configUtil.isAutoUpdateInjectedSpringPropertiesEnabled() && beanFactory instanceof BeanDefinitionRegistry) {
            beanName2SpringValueDefinitions = SpringValueDefinitionProcessor.getBeanName2SpringValueDefinitions((BeanDefinitionRegistry) beanFactory);

        }
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName)
            throws BeansException {
        if (configUtil.isAutoUpdateInjectedSpringPropertiesEnabled()) {
            super.postProcessBeforeInitialization(bean, beanName);
            processBeanPropertyValues(bean, beanName);
        }
        return bean;
    }

    @Override
    protected void processField(Object bean, String beanName, Field field) {
        // register @Value on field
        Value value = field.getAnnotation(Value.class);
        if (value == null) {
            return;
        }

        doRegister(bean, beanName, field, value);
    }

    @Override
    protected void processMethod(Object bean, String beanName, Method method) {
        //register @Value on method
        Value value = method.getAnnotation(Value.class);
        if (value == null) {
            return;
        }
        //skip Configuration bean methods
        if (method.getAnnotation(Bean.class) != null) {
            return;
        }
        if (method.getParameterTypes().length != 1) {
            logger.error("Ignore @Value setter {}.{}, expecting 1 parameter, actual {} parameters",
                    bean.getClass().getName(), method.getName(), method.getParameterTypes().length);
            return;
        }

        doRegister(bean, beanName, method, value);
    }
    // 创建一个 SpringValue 对象，并将其注册到 springValueRegistry 中，后续用于配置更新。
    private void doRegister(Object bean, String beanName, Member member, Value value) {
        Set<String> keys = placeholderHelper.extractPlaceholderKeys(value.value()); // [order.entry.namespace]
        if (keys.isEmpty()) {
            return;
        }
        // [order.entry.namespace]
        for (String key : keys) {
            SpringValue springValue;
            if (member instanceof Field) {
                Field field = (Field) member; // private java.lang.String com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.orderEntry2Namespace
                springValue = new SpringValue(key, value.value(), bean, beanName, field, false);
            } else if (member instanceof Method) {
                Method method = (Method) member;
                springValue = new SpringValue(key, value.value(), bean, beanName, method, false);
            } else {
                logger.error("Apollo @Value annotation currently only support to be used on methods and fields, "
                        + "but is used on {}", member.getClass());
                return;
            }
            springValueRegistry.register(beanFactory, key, springValue);
            logger.info("Monitoring {}", springValue);
        }
    }

    private void processBeanPropertyValues(Object bean, String beanName) {
        Collection<SpringValueDefinition> propertySpringValues = beanName2SpringValueDefinitions
                .get(beanName);
        if (propertySpringValues == null || propertySpringValues.isEmpty()) {
            return;
        }

        for (SpringValueDefinition definition : propertySpringValues) {
            try {
                PropertyDescriptor pd = BeanUtils
                        .getPropertyDescriptor(bean.getClass(), definition.getPropertyName());
                Method method = pd.getWriteMethod();
                if (method == null) {
                    continue;
                }
                SpringValue springValue = new SpringValue(definition.getKey(), definition.getPlaceholder(),
                        bean, beanName, method, false);
                springValueRegistry.register(beanFactory, definition.getKey(), springValue);
                logger.debug("Monitoring {}", springValue);
            } catch (Throwable ex) {
                logger.error("Failed to enable auto update feature for {}.{}", bean.getClass(),
                        definition.getPropertyName());
            }
        }

        // clear
        beanName2SpringValueDefinitions.removeAll(beanName);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
}
