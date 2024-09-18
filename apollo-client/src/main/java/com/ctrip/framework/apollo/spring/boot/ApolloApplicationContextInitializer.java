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
package com.ctrip.framework.apollo.spring.boot;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ApolloClientSystemConsts;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.DeferredLogger;
import com.ctrip.framework.apollo.spring.config.CachedCompositePropertySource;
import com.ctrip.framework.apollo.spring.config.ConfigPropertySourceFactory;
import com.ctrip.framework.apollo.spring.config.PropertySourcesConstants;
import com.ctrip.framework.apollo.spring.util.PropertySourcesUtil;
import com.ctrip.framework.apollo.spring.util.SpringInjector;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.env.CompositePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;

/**
 * Initialize apollo system properties and inject the Apollo config in Spring Boot bootstrap phase
 *
 * <p>Configuration example:</p>
 * <pre class="code">
 *   # set app.id
 *   app.id = 100004458
 *   # enable apollo bootstrap config and inject 'application' namespace in bootstrap phase
 *   apollo.bootstrap.enabled = true
 * </pre>
 * <p>
 * or
 *
 * <pre class="code">
 *   # set app.id
 *   app.id = 100004458
 *   # enable apollo bootstrap config
 *   apollo.bootstrap.enabled = true
 *   # will inject 'application' and 'FX.apollo' namespaces in bootstrap phase
 *   apollo.bootstrap.namespaces = application,FX.apollo
 * </pre>
 * <p>
 * <p>
 * If you want to load Apollo configurations even before Logging System Initialization Phase,
 * add
 * <pre class="code">
 *   # set apollo.bootstrap.eagerLoad.enabled
 *   apollo.bootstrap.eagerLoad.enabled = true
 * </pre>
 * <p>
 * This would be very helpful when your logging configurations is set by Apollo.
 * <p>
 * for example, you have defined logback-spring.xml in your project, and you want to inject some attributes into logback-spring.xml.
 *
 *
 * https://www.cnblogs.com/bigcoder84/p/18213911#gallery-2
 *
 * EnvironmentPostProcessor：当我们想在Bean中使用配置属性时，那么我们的配置属性必须在Bean实例化之前就放入到Spring到Environment中。即我们的接口需要在 application context refreshed 之前进行调用，而 EnvironmentPostProcessor 正好可以实现这个功能
 * 是Spring框架原有的概念，这个类的主要目的就是在 ConfigurableApplicationContext 类型（或者子类型）的 ApplicationContext 做refresh之前，允许我们对 ConfigurableApplicationContext 的实例做进一步的设置或者处理。
 * 两者虽都实现在 Application Context 做 refresh 之前加载配置，但是 EnvironmentPostProcessor 的扩展点相比 ApplicationContextInitializer 更加靠前，使得 Apollo 配置加载能够提到初始化日志系统之前
 *
 *
 * 总的来说，调用顺序如下：
 * 1、initializeSystemProperty 在 postProcessEnvironment 中被调用。
 * 2、postProcessEnvironment 在 Spring 应用环境准备好后被调用，如果配置了 apollo.bootstrap.eagerLoad.enabled=true。
 * 3、initialize 在 postProcessEnvironment 中被调用，如果 apollo.bootstrap.enabled=true。
 * 这个顺序确保了系统属性首先被初始化，然后是 Apollo 配置的加载，这样可以在 Spring 应用启动的早期阶段就使用 Apollo 配置
 */
public class ApolloApplicationContextInitializer implements
        ApplicationContextInitializer<ConfigurableApplicationContext>, EnvironmentPostProcessor, Ordered {
    public static final int DEFAULT_ORDER = 0;

    private static final Logger logger = LoggerFactory.getLogger(ApolloApplicationContextInitializer.class);
    private static final Splitter NAMESPACE_SPLITTER = Splitter.on(",").omitEmptyStrings()
            .trimResults();
    public static final String[] APOLLO_SYSTEM_PROPERTIES = {ApolloClientSystemConsts.APP_ID,
            ApolloClientSystemConsts.APOLLO_LABEL,
            ApolloClientSystemConsts.APOLLO_CLUSTER,
            ApolloClientSystemConsts.APOLLO_CACHE_DIR,
            ApolloClientSystemConsts.APOLLO_ACCESS_KEY_SECRET,
            ApolloClientSystemConsts.APOLLO_META,
            ApolloClientSystemConsts.APOLLO_CONFIG_SERVICE,
            ApolloClientSystemConsts.APOLLO_PROPERTY_ORDER_ENABLE,
            ApolloClientSystemConsts.APOLLO_PROPERTY_NAMES_CACHE_ENABLE,
            ApolloClientSystemConsts.APOLLO_OVERRIDE_SYSTEM_PROPERTIES};

    private final ConfigPropertySourceFactory configPropertySourceFactory = SpringInjector
            .getInstance(ConfigPropertySourceFactory.class);

    private int order = DEFAULT_ORDER;


    @Override
    public void initialize(ConfigurableApplicationContext context) { // org.springframework.boot.web.servlet.context.AnnotationConfigServletWebServerApplicationContext
        // ApplicationServletEnvironment {activeProfiles=[], defaultProfiles=[default], propertySources=[ConfigurationPropertySourcesPropertySource {name='configurationProperties'}, CompositePropertySource {name='ApolloBootstrapPropertySources', propertySources=[ConfigPropertySource {name='application'}]}, StubPropertySource {name='servletConfigInitParams'}, StubPropertySource {name='servletContextInitParams'}, PropertiesPropertySource {name='systemProperties'}, OriginAwareSystemEnvironmentPropertySource {name='systemEnvironment'}, RandomValuePropertySource {name='random'}, OriginTrackedMapPropertySource {name='Config resource 'class path resource [application.properties]' via location 'optional:classpath:/''}]}
        ConfigurableEnvironment environment = context.getEnvironment();
        // 判断是否配置了 apollo.bootstrap.enabled=true
        if (!environment.getProperty(PropertySourcesConstants.APOLLO_BOOTSTRAP_ENABLED, Boolean.class, false)) {
            logger.debug("Apollo bootstrap config is not enabled for context {}, see property: ${{}}", context, PropertySourcesConstants.APOLLO_BOOTSTRAP_ENABLED);
            return;
        }
        logger.debug("Apollo bootstrap config is enabled for context {}", context);

        // 初始化Apollo配置，内部会加载Apollo Server配置
        initialize(environment);
    }


    /**
     * Initialize Apollo Configurations Just after environment is ready.
     * 初始化Apollo配置
     *
     * https://www.cnblogs.com/bigcoder84/p/18213911
     * @param environment
     */
    protected void initialize(ConfigurableEnvironment environment) {
        final ConfigUtil configUtil = ApolloInjector.getInstance(ConfigUtil.class); // 获取单例之前初始化ConfigUtil
        if (environment.getPropertySources().contains(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME)) {
            // 已经初始化，重播日志系统初始化之前打印的日志
            DeferredLogger.replayTo();
            if (configUtil.isOverrideSystemProperties()) {
                // 确保ApolloBootstrapPropertySources仍然是第一个，如果不是会将其调整为第一个，这样从Apollo加载出来的配置拥有更高优先级
                PropertySourcesUtil.ensureBootstrapPropertyPrecedence(environment);
            }
            // 因为有两个不同的触发点，所以该方法首先检查 Spring 的 Environment 环境中是否已经有了 key 为 ApolloBootstrapPropertySources 的目标属性，有的话就不必往下处理，直接 return
            return;
        }

        // 获取配置的命名空间参数
        String namespaces = environment.getProperty(PropertySourcesConstants.APOLLO_BOOTSTRAP_NAMESPACES, ConfigConsts.NAMESPACE_APPLICATION);
        logger.debug("Apollo bootstrap namespaces: {}", namespaces);
        // 使用","切分命名参数
        List<String> namespaceList = NAMESPACE_SPLITTER.splitToList(namespaces);

        CompositePropertySource composite;
        if (configUtil.isPropertyNamesCacheEnabled()) {
            composite = new CachedCompositePropertySource(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME);
        } else {
            // 创建 CompositePropertySource 复合属性源，因为 apollo-client 启动时可以加载多个命名空间的配置，每个命名空间对应一个 PropertySource，
            // 而多个 PropertySource 就会被封装在 CompositePropertySource 对象中，若需要获取apollo中配置的属性时，就会遍历多个命名空间所对应的 PropertySource，
            // 找到对应属性后就会直接返回，这也意味着，先加载的 namespace 中的配置具有更高优先级
            composite = new CompositePropertySource(PropertySourcesConstants.APOLLO_BOOTSTRAP_PROPERTY_SOURCE_NAME);
        }
        for (String namespace : namespaceList) {
            // 从远端拉去命名空间对应的配置
            Config config = ConfigService.getConfig(namespace); // DefaultConfig
            // 调用ConfigPropertySourceFactory#getConfigPropertySource() 缓存从远端拉取的配置，并将其包装为 PropertySource，
            // 最终将所有拉取到的远端配置聚合到一个以 ApolloBootstrapPropertySources 为 key 的属性源包装类 CompositePropertySource 的内部
            composite.addPropertySource(configPropertySourceFactory.getConfigPropertySource(namespace, config));
        }
        // 这段代码的目的是在不覆盖系统属性的前提下，将自定义的属性源添加到Spring环境中，并且确保其优先级低于系统环境属性源
        if (!configUtil.isOverrideSystemProperties()) {
            // 这段代码检查Spring环境的属性源中是否已经包含了系统环境属性
            if (environment.getPropertySources().contains(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME)) {
                // 这段代码将自定义的属性源（composite）添加到Spring环境的属性源中，并且放在系统环境属性源之后。这样做是为了确保自定义的属性源不会覆盖系统属性源
                environment.getPropertySources().addAfter(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, composite);
                return;
            }
        }
        // 将 CompositePropertySource 属性源包装类添加到 Spring 的 Environment 环境中，注意是插入在属性源列表的头部，
        // 因为取属性的时候其实是遍历这个属性源列表来查找，找到即返回，所以出现同名属性时，前面的优先级更高
        // 将一个名为 composite 的 CompositePropertySource 对象添加到 Spring 应用程序的环境（ConfigurableEnvironment）中的属性源（PropertySource）列表的首位
        // 这段代码的目的是将一个包含了重要配置信息的 CompositePropertySource 对象添加到 Spring 应用程序的环境中，并且确保这些配置信息在解析属性时具有最高的优先级
        environment.getPropertySources().addFirst(composite);
    }

    /**
     * To fill system properties from environment config
     */
    void initializeSystemProperty(ConfigurableEnvironment environment) {
        for (String propertyName : APOLLO_SYSTEM_PROPERTIES) {
            fillSystemPropertyFromEnvironment(environment, propertyName);
        }
    }
    // 从环境中获取指定属性名的属性值，并将其设置为系统属性
    private void fillSystemPropertyFromEnvironment(ConfigurableEnvironment environment, String propertyName) {
        if (System.getProperty(propertyName) != null) {
            return;
        }

        String propertyValue = environment.getProperty(propertyName);

        if (Strings.isNullOrEmpty(propertyValue)) {
            return;
        }

        System.setProperty(propertyName, propertyValue);
    }

    /**
     * In order to load Apollo configurations as early as even before Spring loading logging system phase,
     * this EnvironmentPostProcessor can be called Just After ConfigFileApplicationListener has succeeded.
     * <p>
     * <br />
     * The processing sequence would be like this: <br />
     * Load Bootstrap properties and application properties -----> load Apollo configuration properties ----> Initialize Logging systems
     * 为了早在Spring加载日志系统阶段之前就加载Apollo配置，这个EnvironmentPostProcessor可以在ConfigFileApplicationListener成功之后调用。
     * 处理顺序是这样的: 加载Bootstrap属性和应用程序属性----->加载Apollo配置属性---->初始化日志系
     * @param configurableEnvironment
     * @param springApplication
     */
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment configurableEnvironment, SpringApplication springApplication) {
        // 首先，你应该始终在最前面初始化系统属性，比如app.id
        // should always initialize system properties like app.id in the first place
        initializeSystemProperty(configurableEnvironment);

        // 获取 apollo.bootstrap.eagerLoad.enabled 配置
        Boolean eagerLoadEnabled = configurableEnvironment.getProperty(PropertySourcesConstants.APOLLO_BOOTSTRAP_EAGER_LOAD_ENABLED, Boolean.class, false);

        // 如果你不想在日志系统初始化之前进行阿波罗加载，就不应该触发EnvironmentPostProcessor
        if (!eagerLoadEnabled) {
            // 如果未开启提前加载，则 postProcessEnvironment 扩展点直接返回，不加载配置
            return;
        }

        // 是否开启了 apollo.bootstrap.enabled 参数，只有开启了才会在Spring启动阶段加载配置
        Boolean bootstrapEnabled = configurableEnvironment.getProperty(PropertySourcesConstants.APOLLO_BOOTSTRAP_ENABLED, Boolean.class, false);

        if (bootstrapEnabled) {
            DeferredLogger.enable(); // 用于启用延迟日志记录
            // 初始化Apollo配置，内部会加载Apollo Server配置
            initialize(configurableEnvironment);
        }

    }

    /**
     * @since 1.3.0
     */
    @Override
    public int getOrder() {
        return order;
    }

    /**
     * @since 1.3.0
     */
    public void setOrder(int order) {
        this.order = order;
    }

}
