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

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.springframework.core.MethodParameter;

/**
 * Spring @Value method info
 *
 * @author github.com/zhegexiaohuozi  seimimaster@gmail.com
 * @since 2018/2/6.
 * <p>
 * SpringValue是Apollo维护的来更新spring中@Value值变化后的类。里面维护了@Value的占位符，bean，field等信息。
 * 通过继承BeanPostProcessor实现postProcessBeforeInitialization来维护起来的
 */
public class SpringValue {

    private MethodParameter methodParameter;
    private Field field;
    private WeakReference<Object> beanRef;
    private String beanName;
    private String key;
    private String placeholder;
    private Class<?> targetType;
    private Type genericType;
    private boolean isJson;
    // 使用注入属性的时候使用
    public SpringValue(String key, String placeholder, Object bean, String beanName, Field field, boolean isJson) {
        this.beanRef = new WeakReference<>(bean); // 弱引用，有个定时任务【com.ctrip.framework.apollo.spring.property.SpringValueRegistry.scanAndClean】回收，防止内存泄漏，bean=com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController
        this.beanName = beanName; // assignedRoutingKeyController
        this.field = field; // private java.lang.String com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.orderEntry2Namespace
        this.key = key; // order.entry.namespace
        this.placeholder = placeholder; // ${order.entry.namespace:OrderEntryAssignedRouteKeys}
        this.targetType = field.getType(); // java.lang.String
        this.isJson = isJson; // false
        if (isJson) {
            this.genericType = field.getGenericType(); // private java.lang.String com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.orderEntry2Namespace
        }
    }

    /**
     * 使用注入方法的时候使用
     *
     * @param key student
     * @param placeholder ${student}
     * @param bean com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$$EnhancerBySpringCGLIB$$3bed99d4@3502017d
     * @param beanName application
     * @param method public void com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application.setValue3(com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$Student)
     * @param isJson true
     */
    public SpringValue(String key, String placeholder, Object bean, String beanName, Method method, boolean isJson) {
        this.beanRef = new WeakReference<>(bean);
        this.beanName = beanName; //application
        this.methodParameter = new MethodParameter(method, 0); //
        this.key = key; //student
        this.placeholder = placeholder; // ${student}
        Class<?>[] paramTps = method.getParameterTypes(); // class com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$Student
        this.targetType = paramTps[0]; // class com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$Student
        this.isJson = isJson; // true
        if (isJson) {
            this.genericType = method.getGenericParameterTypes()[0]; // class com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$Student
        }
    }

    /**
     * 其实就是使用反射机制运行时修改 Bean 对象中的成员变量，至此自动更新完成
     *
     * @param newVal
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public void update(Object newVal) throws IllegalAccessException, InvocationTargetException {
        if (isField()) {
            injectField(newVal);
        } else {
            injectMethod(newVal);
        }
    }

    private void injectField(Object newVal) throws IllegalAccessException {
        Object bean = beanRef.get();
        if (bean == null) {
            return;
        }
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        field.set(bean, newVal);
        field.setAccessible(accessible);
    }

    private void injectMethod(Object newVal)
            throws InvocationTargetException, IllegalAccessException {
        Object bean = beanRef.get();
        if (bean == null) {
            return;
        }
        methodParameter.getMethod().invoke(bean, newVal);
    }

    public String getBeanName() {
        return beanName;
    }

    public Class<?> getTargetType() {
        return targetType;
    }

    public String getPlaceholder() {
        return this.placeholder;
    }

    public MethodParameter getMethodParameter() {
        return methodParameter;
    }

    public boolean isField() {
        return this.field != null;
    }

    public Field getField() {
        return field;
    }

    public Type getGenericType() {
        return genericType;
    }

    public boolean isJson() {
        return isJson;
    }

    boolean isTargetBeanValid() {
        return beanRef.get() != null;
    }

    @Override
    public String toString() {
        Object bean = beanRef.get();
        if (bean == null) {
            return "";
        }
        if (isField()) {
            return String
                    .format("key: %s, beanName: %s, field: %s.%s", key, beanName, bean.getClass().getName(), field.getName());
        }
        return String.format("key: %s, beanName: %s, method: %s.%s", key, beanName, bean.getClass().getName(),
                methodParameter.getMethod().getName());
    }
}
