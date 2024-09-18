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
package com.ctrip.framework.apollo.spring.spi;

import com.ctrip.framework.apollo.core.spi.Ordered;
import com.ctrip.framework.apollo.spring.annotation.ApolloAnnotationProcessor;
import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import com.ctrip.framework.apollo.spring.annotation.SpringValueProcessor;
import com.ctrip.framework.apollo.spring.config.PropertySourcesProcessor;
import com.ctrip.framework.apollo.spring.property.AutoUpdateConfigChangeListener;
import com.ctrip.framework.apollo.spring.property.SpringValueDefinitionProcessor;
import com.ctrip.framework.apollo.spring.util.BeanRegistrationUtil;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

public class DefaultApolloConfigRegistrarHelper implements ApolloConfigRegistrarHelper {
    private static final Logger logger = LoggerFactory.getLogger(
            DefaultApolloConfigRegistrarHelper.class);
    // ApplicationServletEnvironment {activeProfiles=[], defaultProfiles=[default], propertySources=[CompositePropertySource {name='ApolloBootstrapPropertySources', propertySources=[ConfigPropertySource {name='application'}]}, ConfigurationPropertySourcesPropertySource {name='configurationProperties'}, StubPropertySource {name='servletConfigInitParams'}, StubPropertySource {name='servletContextInitParams'}, PropertiesPropertySource {name='systemProperties'}, OriginAwareSystemEnvironmentPropertySource {name='systemEnvironment'}, RandomValuePropertySource {name='random'}, OriginTrackedMapPropertySource {name='Config resource 'class path resource [application.properties]' via location 'optional:classpath:/''}]}
    private Environment environment;
    // importingClassMetadata=com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application 这个表示@import注解所在的类的元数据
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        // 获取@EnableApolloConfig 注解
        AnnotationAttributes attributes = AnnotationAttributes // {order=2147483647, value=[application, OrderEntryAssignedRouteKeys, OesSiteExecRptAssignedRouteKeys]}
                .fromMap(importingClassMetadata.getAnnotationAttributes(EnableApolloConfig.class.getName()));
        // 获取@EnableApolloConfig 注解的 value 字段
        final String[] namespaces = attributes.getStringArray("value");
        // 获取@EnableApolloConfig 注解的 order 字段
        final int order = attributes.getNumber("order");
        // 解析@EnableApolloConfig 注解的 value 字段
        final String[] resolvedNamespaces = this.resolveNamespaces(namespaces); // ["application", "OrderEntryAssig...", "OesSiteExecRptA..."]
        // 注册 namespace 到 PropertySourcesProcessor 中
        PropertySourcesProcessor.addNamespaces(Lists.newArrayList(resolvedNamespaces), order);
        //
        Map<String, Object> propertySourcesPlaceholderPropertyValues = new HashMap<>();
        //
        // to make sure the default PropertySourcesPlaceholderConfigurer's priority is higher than PropertyPlaceholderConfigurer
        // 根据您提供的上下文，您正在编辑的代码部分是关于确保PropertySourcesPlaceholderConfigurer的优先级高于PropertyPlaceholderConfigurer。这通常是在Spring Boot应用程序中配置Apollo来处理占位符时的一个重要步骤。
        // 为了实现这一点，您需要设置PropertySourcesPlaceholderConfigurer的order属性为一个较低的值，这样它就会在PropertyPlaceholderConfigurer之前被处理
        // PropertySourcesPlaceholderConfigurer的属性配置
        propertySourcesPlaceholderPropertyValues.put("order", 0); // 数字越小，优先级越高

        // PropertySourcesPlaceholderConfigurer是SpringBoot框架自身的占位符处理配置，占位符的处理主要是将 ${apollo.value} 这样的字符串解析出 关键字 apollo.value，再使用这个 key 通过 PropertySourcesPropertyResolver 从 PropertySource 中找到对应的属性值替换掉占位符
        BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, PropertySourcesPlaceholderConfigurer.class, propertySourcesPlaceholderPropertyValues);
        // 注册该监听器就是用于监听 ApolloConfigChangeEvent 事件，当属性发生变更调用 AutoUpdateConfigChangeListener#onChange 方法
        BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, AutoUpdateConfigChangeListener.class);
        // 用于拉取 @EnableApolloConfig 配置的 namespace 的远程配置
        BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, PropertySourcesProcessor.class);
        // 用于处理 Apollo 的专用注解
        BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, ApolloAnnotationProcessor.class);
        // 用于处理 @Value 注解标注的类成员变量和对象方法
        BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, SpringValueProcessor.class);
        // 用于处理 XML 文件中的占位符
        BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, SpringValueDefinitionProcessor.class);
    }

    private String[] resolveNamespaces(String[] namespaces) {
        // no support for Spring version prior to 3.2.x, see https://github.com/apolloconfig/apollo/issues/4178
        if (this.environment == null) {
            //
            logNamespacePlaceholderNotSupportedMessage(namespaces);
            return namespaces;
        }
        String[] resolvedNamespaces = new String[namespaces.length];
        for (int i = 0; i < namespaces.length; i++) {
            // throw IllegalArgumentException if given text is null or if any placeholders are unresolvable
            // 这里的 namespace 是指 @EnableApolloConfig 注解的 value 字段（可以用${}从环境变量中获取）
            resolvedNamespaces[i] = this.environment.resolveRequiredPlaceholders(namespaces[i]);
        }
        return resolvedNamespaces;
    }

    private void logNamespacePlaceholderNotSupportedMessage(String[] namespaces) {
        for (String namespace : namespaces) {
            if (namespace.contains("${")) {
                logger.warn("Namespace placeholder {} is not supported for Spring version prior to 3.2.x,"
                                + " see https://github.com/apolloconfig/apollo/issues/4178 for more details.",
                        namespace);
                break;
            }
        }
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    // 在上下文初始化之前。此时，我们可以通过 Environment 来获取 application.properties 中配置的属性值
    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}