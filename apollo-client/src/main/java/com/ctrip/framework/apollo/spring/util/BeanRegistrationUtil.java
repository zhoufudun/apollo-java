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
package com.ctrip.framework.apollo.spring.util;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.core.type.MethodMetadata;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class BeanRegistrationUtil {
    // reserved bean definitions, we should consider drop this if we will upgrade Spring version
    private static final Map<String, String> RESERVED_BEAN_DEFINITIONS = new ConcurrentHashMap<>();

    static {
        // spring boot property sources placeholder configurer 特殊的BeanDefinition，不能动他
        RESERVED_BEAN_DEFINITIONS.put(
                "org.springframework.context.support.PropertySourcesPlaceholderConfigurer",
                "org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration#propertySourcesPlaceholderConfigurer"
        );
    }

    public static boolean registerBeanDefinitionIfNotExists(BeanDefinitionRegistry registry, Class<?> beanClass) {
        return registerBeanDefinitionIfNotExists(registry, beanClass, null);
    }

    public static boolean registerBeanDefinitionIfNotExists(BeanDefinitionRegistry registry, Class<?> beanClass,
                                                            Map<String, Object> extraPropertyValues) {
        return registerBeanDefinitionIfNotExists(registry, beanClass.getName(), beanClass, extraPropertyValues);
    }

    public static boolean registerBeanDefinitionIfNotExists(BeanDefinitionRegistry registry, String beanName,
                                                            Class<?> beanClass) {
        return registerBeanDefinitionIfNotExists(registry, beanName, beanClass, null);
    }

    public static boolean registerBeanDefinitionIfNotExists(BeanDefinitionRegistry registry, String beanName,
                                                            Class<?> beanClass, Map<String, Object> extraPropertyValues) {
        if (registry.containsBeanDefinition(beanName)) {
            return false;
        }
        // check if there is a bean definition with the same class name
        String[] candidates = registry.getBeanDefinitionNames();
        // 保留的bean definition，不动
        String reservedBeanDefinition = RESERVED_BEAN_DEFINITIONS.get(beanClass.getName());
        for (String candidate : candidates) {
            BeanDefinition beanDefinition = registry.getBeanDefinition(candidate);
            // 如果有相同的class name，直接返回
            if (Objects.equals(beanDefinition.getBeanClassName(), beanClass.getName())) {
                return false;
            }

            // 如果有相同的reserved bean definition，直接返回
            if (reservedBeanDefinition != null && beanDefinition.getSource() != null && beanDefinition.getSource() instanceof MethodMetadata) {
                MethodMetadata metadata = (MethodMetadata) beanDefinition.getSource();
                // 如果是同一个reserved bean definition，直接返回
                if (Objects.equals(reservedBeanDefinition, String.format("%s#%s", metadata.getDeclaringClassName(), metadata.getMethodName()))) {
                    return false;
                }
            }
        }
        // 举例：Generic bean: class [com.ctrip.framework.apollo.spring.annotation.SpringValueProcessor]; scope=; abstract=false; lazyInit=null; autowireMode=0; dependencyCheck=0; autowireCandidate=true; primary=false; factoryBeanName=null; factoryMethodName=null; initMethodName=null; destroyMethodName=null
        // 举例：Generic bean: class [com.ctrip.framework.apollo.spring.annotation.ApolloAnnotationProcessor]; scope=; abstract=false; lazyInit=null; autowireMode=0; dependencyCheck=0; autowireCandidate=true; primary=false; factoryBeanName=null; factoryMethodName=null; initMethodName=null; destroyMethodName=null
        BeanDefinition beanDefinition = BeanDefinitionBuilder.genericBeanDefinition(beanClass).getBeanDefinition();

        if (extraPropertyValues != null) {
            for (Map.Entry<String, Object> entry : extraPropertyValues.entrySet()) {
                beanDefinition.getPropertyValues().add(entry.getKey(), entry.getValue());
            }
        }

        registry.registerBeanDefinition(beanName, beanDefinition);

        return true;
    }

}