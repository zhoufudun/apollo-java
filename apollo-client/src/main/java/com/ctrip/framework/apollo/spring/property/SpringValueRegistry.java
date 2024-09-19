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
 *
 * SpringValueRegistry对象里面封装着含有占位符${key}中key和属性或方法的映射关系，所有只要找到哪个key值有变更，解析变更后的新值，再取出对应要更新的属性或方法，反射设置新值
 *
 * https://blog.51cto.com/u_14691/11376981
 *
 * 这个类的作用参考：https://blog.51cto.com/u_14691/11376981
 */
public class SpringValueRegistry {
    private static final Logger logger = LoggerFactory.getLogger(SpringValueRegistry.class);
    private static final long CLEAN_INTERVAL_IN_SECONDS = 5;

    // Multimap是一个key对应多个实体value的map集合，存放过程中自动将实体进行归
    /**
     * key=org.springframework.beans.factory.support.DefaultListableBeanFactory@772861aa: defining beans [org.springframework.context.annotation.internalConfigurationAnnotationProcessor,org.springframework.context.annotation.internalAutowiredAnnotationProcessor,org.springframework.context.annotation.internalCommonAnnotationProcessor,org.springframework.context.event.internalEventListenerProcessor,org.springframework.context.event.internalEventListenerFactory,application,org.springframework.boot.autoconfigure.internalCachingMetadataReaderFactory,applicationContextUtil,loggerConfiguration,assignedRoutingKeyController,multiNameSpaceConfig,getMyConfig,org.springframework.boot.autoconfigure.AutoConfigurationPackages,org.springframework.context.support.PropertySourcesPlaceholderConfigurer,com.ctrip.framework.apollo.spring.property.AutoUpdateConfigChangeListener,com.ctrip.framework.apollo.spring.config.PropertySourcesProcessor,com.ctrip.framework.apollo.spring.annotation.ApolloAnnotationProcessor,com.ctrip.framework.apollo.spring.annotation.SpringValueProcessor,com.ctrip.framework.apollo.spring.property.SpringValueDefinitionProcessor,org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration,org.springframework.boot.autoconfigure.websocket.servlet.WebSocketServletAutoConfiguration$TomcatWebSocketConfiguration,websocketServletWebServerCustomizer,org.springframework.boot.autoconfigure.websocket.servlet.WebSocketServletAutoConfiguration,org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryConfiguration$EmbeddedTomcat,tomcatServletWebServerFactory,org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration,servletWebServerFactoryCustomizer,tomcatServletWebServerFactoryCustomizer,org.springframework.boot.context.properties.ConfigurationPropertiesBindingPostProcessor,org.springframework.boot.context.internalConfigurationPropertiesBinderFactory,org.springframework.boot.context.internalConfigurationPropertiesBinder,org.springframework.boot.context.properties.BoundConfigurationProperties,org.springframework.boot.context.properties.EnableConfigurationPropertiesRegistrar.methodValidationExcludeFilter,server-org.springframework.boot.autoconfigure.web.ServerProperties,webServerFactoryCustomizerBeanPostProcessor,errorPageRegistrarBeanPostProcessor,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration$DispatcherServletConfiguration,dispatcherServlet,spring.mvc-org.springframework.boot.autoconfigure.web.servlet.WebMvcProperties,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration$DispatcherServletRegistrationConfiguration,dispatcherServletRegistration,org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration,org.springframework.boot.autoconfigure.task.TaskExecutionAutoConfiguration,taskExecutorBuilder,applicationTaskExecutor,spring.task.execution-org.springframework.boot.autoconfigure.task.TaskExecutionProperties,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration$WhitelabelErrorViewConfiguration,error,beanNameViewResolver,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration$DefaultErrorViewResolverConfiguration,conventionErrorViewResolver,spring.resources-org.springframework.boot.autoconfigure.web.ResourceProperties,spring.web-org.springframework.boot.autoconfigure.web.WebProperties,org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration,errorAttributes,basicErrorController,errorPageCustomizer,preserveErrorControllerTargetClassPostProcessor,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration$EnableWebMvcConfiguration,requestMappingHandlerAdapter,requestMappingHandlerMapping,welcomePageHandlerMapping,localeResolver,themeResolver,flashMapManager,mvcConversionService,mvcValidator,mvcContentNegotiationManager,mvcPatternParser,mvcUrlPathHelper,mvcPathMatcher,viewControllerHandlerMapping,beanNameHandlerMapping,routerFunctionMapping,resourceHandlerMapping,mvcResourceUrlProvider,defaultServletHandlerMapping,handlerFunctionAdapter,mvcUriComponentsContributor,httpRequestHandlerAdapter,simpleControllerHandlerAdapter,handlerExceptionResolver,mvcViewResolver,mvcHandlerMappingIntrospector,viewNameTranslator,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration$WebMvcAutoConfigurationAdapter,defaultViewResolver,viewResolver,requestContextFilter,org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration,formContentFilter,org.springframework.boot.autoconfigure.availability.ApplicationAvailabilityAutoConfiguration,applicationAvailability,org.springframework.boot.actuate.autoconfigure.availability.AvailabilityHealthContributorAutoConfiguration,org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration$Jackson2ObjectMapperBuilderCustomizerConfiguration,standardJacksonObjectMapperBuilderCustomizer,spring.jackson-org.springframework.boot.autoconfigure.jackson.JacksonProperties,org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration$JacksonObjectMapperBuilderConfiguration,jacksonObjectMapperBuilder,org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration$ParameterNamesModuleConfiguration,parameterNamesModule,org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration$JacksonObjectMapperConfiguration,jacksonObjectMapper,org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration,jsonComponentModule,org.springframework.boot.actuate.autoconfigure.web.servlet.ServletManagementContextAutoConfiguration,servletWebChildContextFactory,managementServletContext,org.springframework.boot.actuate.autoconfigure.health.HealthEndpointConfiguration,healthStatusAggregator,healthHttpCodeStatusMapper,healthEndpointGroups,healthContributorRegistry,healthEndpoint,healthEndpointGroupsBeanPostProcessor,org.springframework.boot.actuate.autoconfigure.health.HealthEndpointWebExtensionConfiguration,healthEndpointWebExtension,org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration,management.endpoint.health-org.springframework.boot.actuate.autoconfigure.health.HealthEndpointProperties,org.springframework.boot.autoconfigure.info.ProjectInfoAutoConfiguration,spring.info-org.springframework.boot.autoconfigure.info.ProjectInfoProperties,org.springframework.boot.actuate.autoconfigure.info.InfoContributorAutoConfiguration,envInfoContributor,management.info-org.springframework.boot.actuate.autoconfigure.info.InfoContributorProperties,org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration,endpointOperationParameterMapper,endpointCachingOperationInvokerAdvisor,org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointAutoConfiguration$WebEndpointServletConfiguration,servletEndpointDiscoverer,org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointAutoConfiguration,webEndpointPathMapper,endpointMediaTypes,webEndpointDiscoverer,controllerEndpointDiscoverer,pathMappedEndpoints,webExposeExcludePropertyEndpointFilter,controllerExposeExcludePropertyEndpointFilter,management.endpoints.web-org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties,org.springframework.boot.actuate.autoconfigure.system.DiskSpaceHealthContributorAutoConfiguration,diskSpaceHealthIndicator,management.health.diskspace-org.springframework.boot.actuate.autoconfigure.system.DiskSpaceHealthIndicatorProperties,org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration,pingHealthContributor,org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration,micrometerClock,meterRegistryPostProcessor,propertiesMeterFilter,management.metrics-org.springframework.boot.actuate.autoconfigure.metrics.MetricsProperties,org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration,simpleMeterRegistry,simpleConfig,management.metrics.export.simple-org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleProperties,org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration,org.springframework.boot.actuate.autoconfigure.metrics.JvmMetricsAutoConfiguration,jvmGcMetrics,jvmMemoryMetrics,jvmThreadMetrics,classLoaderMetrics,org.springframework.boot.actuate.autoconfigure.metrics.LogbackMetricsAutoConfiguration,logbackMetrics,org.springframework.boot.actuate.autoconfigure.metrics.SystemMetricsAutoConfiguration,uptimeMetrics,processorMetrics,fileDescriptorMetrics,org.springframework.boot.actuate.autoconfigure.metrics.integration.IntegrationMetricsAutoConfiguration,org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration,gsonBuilder,gson,standardGsonBuilderCustomizer,spring.gson-org.springframework.boot.autoconfigure.gson.GsonProperties,org.springframework.boot.autoconfigure.http.HttpMessageConvertersAutoConfiguration$StringHttpMessageConverterConfiguration,stringHttpMessageConverter,org.springframework.boot.autoconfigure.http.JacksonHttpMessageConvertersConfiguration$MappingJackson2HttpMessageConverterConfiguration,mappingJackson2HttpMessageConverter,org.springframework.boot.autoconfigure.http.JacksonHttpMessageConvertersConfiguration,org.springframework.boot.autoconfigure.http.GsonHttpMessageConvertersConfiguration,org.springframework.boot.autoconfigure.http.HttpMessageConvertersAutoConfiguration,messageConverters,org.springframework.boot.autoconfigure.web.client.RestTemplateAutoConfiguration,restTemplateBuilderConfigurer,restTemplateBuilder,org.springframework.boot.actuate.autoconfigure.metrics.web.client.RestTemplateMetricsConfiguration,restTemplateExchangeTagsProvider,metricsRestTemplateCustomizer,org.springframework.boot.actuate.autoconfigure.metrics.web.client.HttpClientMetricsAutoConfiguration,metricsHttpClientUriTagFilter,org.springframework.boot.actuate.autoconfigure.metrics.web.servlet.WebMvcMetricsAutoConfiguration,webMvcTagsProvider,webMvcMetricsFilter,metricsHttpServerUriTagFilter,metricsWebMvcConfigurer,org.springframework.boot.actuate.autoconfigure.metrics.web.tomcat.TomcatMetricsAutoConfiguration,tomcatMetricsBinder,org.springframework.boot.autoconfigure.aop.AopAutoConfiguration$AspectJAutoProxyingConfiguration$CglibAutoProxyConfiguration,org.springframework.aop.config.internalAutoProxyCreator,org.springframework.boot.autoconfigure.aop.AopAutoConfiguration$AspectJAutoProxyingConfiguration,org.springframework.boot.autoconfigure.aop.AopAutoConfiguration,org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration,org.springframework.boot.autoconfigure.context.LifecycleAutoConfiguration,lifecycleProcessor,spring.lifecycle-org.springframework.boot.autoconfigure.context.LifecycleProperties,org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration,spring.sql.init-org.springframework.boot.autoconfigure.sql.init.SqlInitializationProperties,org.springframework.boot.sql.init.dependency.DatabaseInitializationDependencyConfigurer$DependsOnDatabaseInitializationPostProcessor,org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration,scheduledBeanLazyInitializationExcludeFilter,taskSchedulerBuilder,spring.task.scheduling-org.springframework.boot.autoconfigure.task.TaskSchedulingProperties,org.springframework.boot.autoconfigure.web.embedded.EmbeddedWebServerFactoryCustomizerAutoConfiguration$TomcatWebServerFactoryCustomizerConfiguration,tomcatWebServerFactoryCustomizer,org.springframework.boot.autoconfigure.web.embedded.EmbeddedWebServerFactoryCustomizerAutoConfiguration,org.springframework.boot.autoconfigure.web.servlet.HttpEncodingAutoConfiguration,characterEncodingFilter,localeCharsetMappingsCustomizer,org.springframework.boot.autoconfigure.web.servlet.MultipartAutoConfiguration,multipartConfigElement,multipartResolver,spring.servlet.multipart-org.springframework.boot.autoconfigure.web.servlet.MultipartProperties,org.springframework.boot.actuate.autoconfigure.endpoint.web.ServletEndpointManagementContextConfiguration$WebMvcServletEndpointManagementContextConfiguration,servletEndpointRegistrar,org.springframework.boot.actuate.autoconfigure.endpoint.web.ServletEndpointManagementContextConfiguration,servletExposeExcludePropertyEndpointFilter,org.springframework.boot.actuate.autoconfigure.endpoint.web.servlet.WebMvcEndpointManagementContextConfiguration,webEndpointServletHandlerMapping,controllerEndpointHandlerMapping,management.endpoints.web.cors-org.springframework.boot.actuate.autoconfigure.endpoint.web.CorsEndpointProperties,org.springframework.boot.actuate.autoconfigure.web.server.ManagementContextAutoConfiguration$SameManagementContextConfiguration$EnableSameManagementContextConfiguration,org.springframework.boot.actuate.autoconfigure.web.server.ManagementContextAutoConfiguration$SameManagementContextConfiguration,org.springframework.boot.actuate.autoconfigure.web.server.ManagementContextAutoConfiguration,management.server-org.springframework.boot.actuate.autoconfigure.web.server.ManagementServerProperties]; root of factory hierarchy
     * value={order.entry.namespace=[key: order.entry.namespace, beanName: assignedRoutingKeyController, field: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.orderEntry2Namespace], oes_site.exec_rpt.namespace=[key: oes_site.exec_rpt.namespace, beanName: assignedRoutingKeyController, field: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.oesExecRpt2Namespace]}
     *
     * Multimap<String, SpringValue> 中key=namespace，value=[key: order.entry.namespace, beanName: assignedRoutingKeyController, field: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.orderEntry2Namespace]
     */
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
    // beanFactory 是一个接口，它的实现类有很多，例如 DefaultListableBeanFactory、XmlBeanFactory 等。  key=变化的key
    public Collection<SpringValue> get(BeanFactory beanFactory, String key) {
        Multimap<String, SpringValue> beanFactorySpringValues = registry.get(beanFactory); // {student=[key: student, beanName: application, method: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$$EnhancerBySpringCGLIB$$c6286d24.setValue3], key=[key: key, beanName: application, method: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.Application$$EnhancerBySpringCGLIB$$c6286d24.setValue2], order.entry.namespace=[key: order.entry.namespace, beanName: assignedRoutingKeyController, field: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.orderEntry2Namespace], oes_site.exec_rpt.namespace=[key: oes_site.exec_rpt.namespace, beanName: assignedRoutingKeyController, field: com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller.AssignedRoutingKeyController.oesExecRpt2Namespace]}
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
