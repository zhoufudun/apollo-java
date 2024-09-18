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

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.framework.apollo.spring.property.PlaceholderHelper;
import com.ctrip.framework.apollo.spring.property.SpringValue;
import com.ctrip.framework.apollo.spring.property.SpringValueRegistry;
import com.ctrip.framework.apollo.spring.util.SpringInjector;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.Environment;
import org.springframework.util.ReflectionUtils;

/**
 * Apollo Annotation Processor for Spring Application
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class ApolloAnnotationProcessor extends ApolloProcessor implements BeanFactoryAware,
        EnvironmentAware {

    private static final Logger logger = LoggerFactory.getLogger(ApolloAnnotationProcessor.class);

    private static final String NAMESPACE_DELIMITER = ",";

    private static final Splitter NAMESPACE_SPLITTER = Splitter.on(NAMESPACE_DELIMITER)
            .omitEmptyStrings().trimResults();
    private static final Map<String, Gson> DATEPATTERN_GSON_MAP = new ConcurrentHashMap<>();

    private final ConfigUtil configUtil;
    private final PlaceholderHelper placeholderHelper;
    private final SpringValueRegistry springValueRegistry;

    /**
     * resolve the expression.
     */
    private ConfigurableBeanFactory configurableBeanFactory;

    private Environment environment;

    public ApolloAnnotationProcessor() {
        configUtil = ApolloInjector.getInstance(ConfigUtil.class);
        placeholderHelper = SpringInjector.getInstance(PlaceholderHelper.class);
        springValueRegistry = SpringInjector.getInstance(SpringValueRegistry.class);
    }

    @Override
    protected void processField(Object bean, String beanName, Field field) {
        this.processApolloConfig(bean, field);
        this.processApolloJsonValue(bean, beanName, field);
    }

    @Override
    protected void processMethod(final Object bean, String beanName, final Method method) {
        this.processApolloConfigChangeListener(bean, method); // 解析@ApolloConfigChangeListener注解
        this.processApolloJsonValue(bean, beanName, method); // 解析@ApolloJsonValue注解
    }
    // field举例：private com.ctrip.framework.apollo.Config com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application.config
    private void processApolloConfig(Object bean, Field field) {
        ApolloConfig annotation = AnnotationUtils.getAnnotation(field, ApolloConfig.class);
        if (annotation == null) {
            return;
        }
        // 需要注入的类型必须是Config.class
        Preconditions.checkArgument(Config.class.isAssignableFrom(field.getType()),
                "Invalid type: %s for field: %s, should be Config", field.getType(), field);

        final String namespace = annotation.value(); // @com.ctrip.framework.apollo.spring.annotation.ApolloConfig("application")
        final String resolvedNamespace = this.environment.resolveRequiredPlaceholders(namespace); //
        Config config = ConfigService.getConfig(resolvedNamespace); // DefaultConfig

        ReflectionUtils.makeAccessible(field);
        ReflectionUtils.setField(field, bean, config);
    }

    private void processApolloConfigChangeListener(final Object bean, final Method method) {
        ApolloConfigChangeListener annotation = AnnotationUtils
                .findAnnotation(method, ApolloConfigChangeListener.class);
        if (annotation == null) {
            return;
        }
        Class<?>[] parameterTypes = method.getParameterTypes(); // method=public void com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.listener(com.ctrip.framework.apollo.model.ConfigChangeEvent)
        Preconditions.checkArgument(parameterTypes.length == 1,
                "Invalid number of parameters: %s for method: %s, should be 1", parameterTypes.length,
                method);
        Preconditions.checkArgument(ConfigChangeEvent.class.isAssignableFrom(parameterTypes[0]),
                "Invalid parameter type: %s for method: %s, should be ConfigChangeEvent", parameterTypes[0],
                method);

        ReflectionUtils.makeAccessible(method); // 给method设置可访问权限
        String[] namespaces = annotation.value(); // ["application", "OesSiteExecRptAssignedRouteKeys"]， annotation=@com.ctrip.framework.apollo.spring.annotation.ApolloConfigChangeListener(value={"application", "OesSiteExecRptAssignedRouteKeys"}, interestedKeyPrefixes={"k"}, interestedKeys={"key"})
        String[] annotatedInterestedKeys = annotation.interestedKeys(); // ["key"]
        String[] annotatedInterestedKeyPrefixes = annotation.interestedKeyPrefixes(); // ["k"]
        ConfigChangeListener configChangeListener = new ConfigChangeListener() {
            @Override
            public void onChange(ConfigChangeEvent changeEvent) {
                ReflectionUtils.invokeMethod(method, bean, changeEvent);
            }
        }; // 创建一个ConfigChangeListener，这个监听器会在监听到配置变更的时候调用method方法

        Set<String> interestedKeys =
                annotatedInterestedKeys.length > 0 ? Sets.newHashSet(annotatedInterestedKeys) : null; // ["key"]
        Set<String> interestedKeyPrefixes =
                annotatedInterestedKeyPrefixes.length > 0 ? Sets.newHashSet(annotatedInterestedKeyPrefixes) // ["k"]
                        : null;

        Set<String> resolvedNamespaces = processResolveNamespaceValue(namespaces);

        for (String namespace : resolvedNamespaces) {
            Config config = ConfigService.getConfig(namespace);

            if (interestedKeys == null && interestedKeyPrefixes == null) {
                config.addChangeListener(configChangeListener);
            } else {
                config.addChangeListener(configChangeListener, interestedKeys, interestedKeyPrefixes);
            }
        }
    }

    /**
     * Evaluate and resolve namespaces from env/properties.
     * Split delimited namespaces
     *
     * @param namespaces
     * @return resolved namespaces
     */
    private Set<String> processResolveNamespaceValue(String[] namespaces) { // ["application", "OesSiteExecRptAssignedRouteKeys"]

        Set<String> resolvedNamespaces = new HashSet<>();

        for (String namespace : namespaces) {
            final String resolvedNamespace = this.environment.resolveRequiredPlaceholders(namespace); // namespace还可以是占位符

            if (resolvedNamespace.contains(NAMESPACE_DELIMITER)) { // 如果namespace中包含分隔符，那么就用分隔符分割。举例：namespace=application,key
                resolvedNamespaces.addAll(NAMESPACE_SPLITTER.splitToList(resolvedNamespace));
            } else {
                resolvedNamespaces.add(resolvedNamespace);
            }
        }

        return resolvedNamespaces;
    }

    private void processApolloJsonValue(Object bean, String beanName, Field field) {
        ApolloJsonValue apolloJsonValue = AnnotationUtils.getAnnotation(field, ApolloJsonValue.class);
        if (apolloJsonValue == null) {
            return;
        }

        String placeholder = apolloJsonValue.value();
        String datePattern = apolloJsonValue.datePattern();
        Object propertyValue = this.resolvePropertyValue(beanName, placeholder); // key
        if (propertyValue == null) {
            return;
        }

        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        ReflectionUtils // 给当前bean的field赋值
                .setField(field, bean, parseJsonValue((String) propertyValue, field.getGenericType(), datePattern));
        field.setAccessible(accessible);

        if (configUtil.isAutoUpdateInjectedSpringPropertiesEnabled()) {
            Set<String> keys = placeholderHelper.extractPlaceholderKeys(placeholder);
            for (String key : keys) {
                SpringValue springValue = new SpringValue(key, placeholder, bean, beanName, field, true);
                springValueRegistry.register(this.configurableBeanFactory, key, springValue);
                logger.debug("Monitoring {}", springValue);
            }
        }
    }
    // method=public void com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application.setValue3(com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$Student)
    private void processApolloJsonValue(Object bean, String beanName, Method method) {
        ApolloJsonValue apolloJsonValue = AnnotationUtils.getAnnotation(method, ApolloJsonValue.class);
        if (apolloJsonValue == null) {
            return;
        }

        String placeHolder = apolloJsonValue.value(); // ${student}
        String datePattern = apolloJsonValue.datePattern(); // “”
        Object propertyValue = this.resolvePropertyValue(beanName, placeHolder); // {"name":"zhoufudn","age":20}
        if (propertyValue == null) {
            return;
        }

        Type[] types = method.getGenericParameterTypes(); // class java.lang.String
        Preconditions.checkArgument(types.length == 1,
                "Ignore @ApolloJsonValue setter {}.{}, expecting 1 parameter, actual {} parameters",
                bean.getClass().getName(), method.getName(), method.getParameterTypes().length);

        boolean accessible = method.isAccessible();
        method.setAccessible(true);
        ReflectionUtils.invokeMethod(method, bean, parseJsonValue((String) propertyValue, types[0], datePattern)); // 反射执行方法
        method.setAccessible(accessible);

        if (configUtil.isAutoUpdateInjectedSpringPropertiesEnabled()) {
            Set<String> keys = placeholderHelper.extractPlaceholderKeys(placeHolder); // student
            for (String key : keys) {
                SpringValue springValue = new SpringValue(key, placeHolder, bean, beanName, method, true);
                springValueRegistry.register(this.configurableBeanFactory, key, springValue);
                logger.debug("Monitoring {}", springValue);
            }
        }
    }

    @Nullable
    private Object resolvePropertyValue(String beanName, String placeHolder) {
        Object propertyValue = placeholderHelper
                .resolvePropertyValue(this.configurableBeanFactory, beanName, placeHolder); // 使用Spring提供的能力来解析占位符

        // propertyValue will never be null, as @ApolloJsonValue will not allow that
        if (!(propertyValue instanceof String)) {
            return null;
        }

        return propertyValue;
    }

    private Object parseJsonValue(String json, Type targetType, String datePattern) {
        try {
            return DATEPATTERN_GSON_MAP.computeIfAbsent(datePattern, this::buildGson).fromJson(json, targetType);
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

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.configurableBeanFactory = (ConfigurableBeanFactory) beanFactory;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
