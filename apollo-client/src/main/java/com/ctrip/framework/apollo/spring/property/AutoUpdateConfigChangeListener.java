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

import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.framework.apollo.spring.annotation.ApolloJsonValue;
import com.ctrip.framework.apollo.spring.events.ApolloConfigChangeEvent;
import com.ctrip.framework.apollo.spring.util.SpringInjector;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.google.gson.Gson;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.gson.GsonBuilder;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.TypeConverter;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.CollectionUtils;

/**
 * Create by zhangzheng on 2018/3/6
 * <p>
 * https://www.cnblogs.com/bigcoder84/p/18213911#gallery-2
 * https://www.cnblogs.com/bigcoder84/p/18213911
 * <p>
 * 在DefaultApolloConfigRegistrarHelper#registerBeanDefinitions
 * 会注册 AutoUpdateConfigChangeListener Bean进入Ioc容器，而该监听器就是用于监听 ApolloConfigChangeEvent 事件，
 * 当属性发生变更调用 AutoUpdateConfigChangeListener#onChange
 *
 */
public class AutoUpdateConfigChangeListener implements ConfigChangeListener,
        ApplicationListener<ApolloConfigChangeEvent>, ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(AutoUpdateConfigChangeListener.class);
    private final boolean typeConverterHasConvertIfNecessaryWithFieldParameter;
    private ConfigurableBeanFactory beanFactory; // org.springframework.beans.factory.support.DefaultListableBeanFactory
    private TypeConverter typeConverter; // SimpleTypeConverter
    private final PlaceholderHelper placeholderHelper;
    private final SpringValueRegistry springValueRegistry;
    private final Map<String, Gson> datePatternGsonMap;
    private final ConfigUtil configUtil;

    public AutoUpdateConfigChangeListener() {
        this.typeConverterHasConvertIfNecessaryWithFieldParameter = testTypeConverterHasConvertIfNecessaryWithFieldParameter();
        this.placeholderHelper = SpringInjector.getInstance(PlaceholderHelper.class);
        this.springValueRegistry = SpringInjector.getInstance(SpringValueRegistry.class);
        this.datePatternGsonMap = new ConcurrentHashMap<>();
        this.configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    }

    @Override
    public void onChange(ConfigChangeEvent changeEvent) {
        Set<String> keys = changeEvent.changedKeys(); // 获取所有的key：[key]
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }
        for (String key : keys) {
            // 1. check whether the changed key is relevant
            Collection<SpringValue> targetValues = springValueRegistry.get(beanFactory, key);
            if (targetValues == null || targetValues.isEmpty()) {
                continue;
            }

            // 2. update the value
            for (SpringValue val : targetValues) { // targetValues=key: key, beanName: application, method: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$$EnhancerBySpringCGLIB$$c6286d24.setValue2
                updateSpringValue(val);
            }
        }
    }

    private void updateSpringValue(SpringValue springValue) {
        try {
            Object value = resolvePropertyValue(springValue); // keyName5
            springValue.update(value);

            logger.info("Auto update apollo changed value successfully, new value: {}, {}", value,
                    springValue);
        } catch (Throwable ex) {
            logger.error("Auto update apollo changed value failed, {}", springValue.toString(), ex);
        }
    }

    /**
     * Logic transplanted from DefaultListableBeanFactory
     * 首先调用 AutoUpdateConfigChangeListener#resolvePropertyValue() 方法借助 SpringBoot 的组件将 @Value 中配置的占位符替换为 PropertySource 中的对应 key 的属性值，
     * 此处涉及到 Spring 创建 Bean 对象时的属性注入机制，比较复杂，暂不作深入分析
     *
     * @see org.springframework.beans.factory.support.DefaultListableBeanFactory#doResolveDependency(org.springframework.beans.factory.config.DependencyDescriptor,
     * java.lang.String, java.util.Set, org.springframework.beans.TypeConverter)
     */
    private Object resolvePropertyValue(SpringValue springValue) { // key: key, beanName: application, method: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$$EnhancerBySpringCGLIB$$c6286d24.setValue2
        // value will never be null, as @Value and @ApolloJsonValue will not allow that
        Object value = placeholderHelper // value=keyName5
                .resolvePropertyValue(beanFactory, springValue.getBeanName(), springValue.getPlaceholder());

        if (springValue.isJson()) { // 是json
            ApolloJsonValue apolloJsonValue = springValue.isField() ?
                    springValue.getField().getAnnotation(ApolloJsonValue.class) :
                    springValue.getMethodParameter().getMethodAnnotation(ApolloJsonValue.class);
            String datePattern = apolloJsonValue != null ? apolloJsonValue.datePattern() : StringUtils.EMPTY;
            value = parseJsonValue((String) value, springValue.getGenericType(), datePattern);
        } else {
            if (springValue.isField()) { // 是字段
                // org.springframework.beans.TypeConverter#convertIfNecessary(java.lang.Object, java.lang.Class, java.lang.reflect.Field) is available from Spring 3.2.0+
                if (typeConverterHasConvertIfNecessaryWithFieldParameter) {
                    value = this.typeConverter
                            .convertIfNecessary(value, springValue.getTargetType(), springValue.getField());
                } else {
                    value = this.typeConverter.convertIfNecessary(value, springValue.getTargetType());
                }
            } else { // 是方法
                value = this.typeConverter.convertIfNecessary(value, springValue.getTargetType(),
                        springValue.getMethodParameter()); // keyName5, 使用类型转换器typeConverter来转换值value，使其符合springValue的目标类型targetType
            }
        }

        return value;
    }

    private Object parseJsonValue(String json, Type targetType, String datePattern) {
        try {
            return datePatternGsonMap.computeIfAbsent(datePattern, this::buildGson).fromJson(json, targetType);
        } catch (Throwable ex) {
            logger.error("Parsing json '{}' to type {} failed!", json, targetType, ex);
            throw ex;
        }
    }

    private Gson buildGson(String datePattern) {
        if (StringUtils.isBlank(datePattern)) {
            return new Gson();
        }
        return new GsonBuilder().setDateFormat(datePattern).create();
    }

    private boolean testTypeConverterHasConvertIfNecessaryWithFieldParameter() {
        try {
            TypeConverter.class.getMethod("convertIfNecessary", Object.class, Class.class, Field.class);
        } catch (Throwable ex) {
            return false;
        }

        return true;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        //it is safe enough to cast as all known application context is derived from ConfigurableApplicationContext
        this.beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        this.typeConverter = this.beanFactory.getTypeConverter();
    }

    // 收到spring发出的事件：ApolloConfigChangeEvent
    @Override
    public void onApplicationEvent(ApolloConfigChangeEvent event) {
        // 检查是否开启了自动更新
        if (!configUtil.isAutoUpdateInjectedSpringPropertiesEnabled()) {
            return;
        }
        this.onChange(event.getConfigChangeEvent());
    }
}
