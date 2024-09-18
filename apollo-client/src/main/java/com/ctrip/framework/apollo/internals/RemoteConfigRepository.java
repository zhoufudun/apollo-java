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
package com.ctrip.framework.apollo.internals;

import com.ctrip.framework.apollo.Apollo;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfig;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.framework.apollo.core.schedule.SchedulePolicy;
import com.ctrip.framework.apollo.core.signature.Signature;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.core.utils.DeferredLoggerFactory;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.enums.ConfigSourceType;
import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.exceptions.ApolloConfigStatusCodeException;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.ctrip.framework.apollo.util.http.HttpRequest;
import com.ctrip.framework.apollo.util.http.HttpResponse;
import com.ctrip.framework.apollo.util.http.HttpClient;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

/**
 * @author Jason Song(song_s@ctrip.com)
 * <p>
 * <p>
 * https://www.cnblogs.com/bigcoder84/p/18213911
 * <p>
 * 每个namespace下的配置，都有一个RemoteConfigRepository对象
 * <p>
 * <p>
 * Spring启动流程创建 RemoteConfigRepository 对象时会尝试第一次拉取namespace对应的配置，
 * 拉取完后会创建定时拉取任务和长轮询任务，长轮询任务调用 RemoteConfigLongPollService#startLongPolling 来实现，
 * 若服务端配置发生变更，则会回调 RemoteConfigRepository#onLongPollNotified 方法，
 * 在这个方法中会调用 RemoteConfigRepository#sync 方法重新拉取对应 namespace 的远端配置
 */
public class RemoteConfigRepository extends AbstractConfigRepository {
    private static final Logger logger = DeferredLoggerFactory.getLogger(RemoteConfigRepository.class);
    private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
    private static final Joiner.MapJoiner MAP_JOINER = Joiner.on("&").withKeyValueSeparator("=");
    private static final Escaper pathEscaper = UrlEscapers.urlPathSegmentEscaper();
    private static final Escaper queryParamEscaper = UrlEscapers.urlFormParameterEscaper();

    private final ConfigServiceLocator m_serviceLocator;
    private final HttpClient m_httpClient;
    private final ConfigUtil m_configUtil;
    /**
     * 远程配置长轮询服务
     */
    private final RemoteConfigLongPollService remoteConfigLongPollService;
    /**
     * 指向ApolloConfig的AtomicReference,拉取的远端配置缓存
     */
    private volatile AtomicReference<ApolloConfig> m_configCache;
    private final String m_namespace;
    private final static ScheduledExecutorService m_executorService;
    private final AtomicReference<ServiceDTO> m_longPollServiceDto;
    private final AtomicReference<ApolloNotificationMessages> m_remoteMessages;
    /**
     * 加载配置的RateLimiter
     */
    private final RateLimiter m_loadConfigRateLimiter;
    /**
     * 是否强制拉取缓存的标记
     * 若为true,则多一轮从Config Service拉取配置
     * 为true的原因:RemoteConfigRepository知道Config Service有配置刷新
     */
    private final AtomicBoolean m_configNeedForceRefresh;
    /**
     * 失败定时重试策略
     */
    private final SchedulePolicy m_loadConfigFailSchedulePolicy;
    private static final Gson GSON = new Gson();

    static {
        m_executorService = Executors.newScheduledThreadPool(1,
                ApolloThreadFactory.create("RemoteConfigRepository", true));
    }

    /**
     * Constructor.
     *
     * @param namespace the namespace
     */
    public RemoteConfigRepository(String namespace) {
        m_namespace = namespace;
        m_configCache = new AtomicReference<>();
        m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
        m_httpClient = ApolloInjector.getInstance(HttpClient.class);
        m_serviceLocator = ApolloInjector.getInstance(ConfigServiceLocator.class);
        remoteConfigLongPollService = ApolloInjector.getInstance(RemoteConfigLongPollService.class);
        m_longPollServiceDto = new AtomicReference<>();
        m_remoteMessages = new AtomicReference<>();
        m_loadConfigRateLimiter = RateLimiter.create(m_configUtil.getLoadConfigQPS());
        m_configNeedForceRefresh = new AtomicBoolean(true);
        m_loadConfigFailSchedulePolicy = new ExponentialSchedulePolicy(m_configUtil.getOnErrorRetryInterval(),
                m_configUtil.getOnErrorRetryInterval() * 8);
        // 初始化定时刷新配置的任务
        this.schedulePeriodicRefresh(); // 主动定时拉取
        // 注册自己到RemoteConfigLongPollService中,实现配置更新的实时通知
        this.scheduleLongPollingRefresh(); // 长轮训获取
    }

    @Override
    public Properties getConfig() {
        if (m_configCache.get() == null) {
            this.sync();
        }
        return transformApolloConfigToProperties(m_configCache.get());
    }

    @Override
    public void setUpstreamRepository(ConfigRepository upstreamConfigRepository) {
        //remote config doesn't need upstream
    }

    @Override
    public ConfigSourceType getSourceType() {
        return ConfigSourceType.REMOTE;
    }

    private void schedulePeriodicRefresh() {
        logger.debug("Schedule periodic refresh with interval: {} {}",
                m_configUtil.getRefreshInterval(), m_configUtil.getRefreshIntervalTimeUnit());
        m_executorService.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        Tracer.logEvent("Apollo.ConfigService", String.format("periodicRefresh: %s", m_namespace));
                        logger.debug("refresh config for namespace: {}", m_namespace);
                        // 同步配置
                        trySync();
                        Tracer.logEvent("Apollo.Client.Version", Apollo.VERSION);
                    }
                    // 默认每5分钟同步一次配置
                }, m_configUtil.getRefreshInterval(), m_configUtil.getRefreshInterval(),
                m_configUtil.getRefreshIntervalTimeUnit());
    }

    @Override
    protected synchronized void sync() {
        Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "syncRemoteConfig");

        try {
            // 缓存的 Apollo服务端配置，之前缓存的配置
            ApolloConfig previous = m_configCache.get(); // ApolloConfig{appId='fix-server', cluster='default', namespaceName='application', configurations={name=zhoufudun, key={"name":"zzz"}}, releaseKey='20240914225607-94c9c979ff238727'}
            // 从Apollo Server加载配置
            ApolloConfig current = loadApolloConfig(); //

            //reference equals means HTTP 304
            if (previous != current) {
                logger.debug("Remote Config refreshed!");
                // 若不相等,说明更新了,设置到缓存中
                m_configCache.set(current);
                // 发布配置变更事件，实际上是回调 LocalFileConfigRepository.onRepositoryChange
                /**
                 * 如果配置发生变更，回调 LocalFileConfigRepository.onRepositoryChange方法，
                 * 从而将最新配置同步到 LocalFileConfigRepository。而 LocalFileConfigRepository 在更新完本地文件缓存配置后，
                 * 同样会回调 DefaultConfig.onRepositoryChange 同步内存缓存。
                 */
                this.fireRepositoryChange(m_namespace, this.getConfig());
            }

            if (current != null) {
                Tracer.logEvent(String.format("Apollo.Client.Configs.%s", current.getNamespaceName()),
                        current.getReleaseKey());
            }

            transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
            transaction.setStatus(ex);
            throw ex;
        } finally {
            transaction.complete();
        }
    }

    private Properties transformApolloConfigToProperties(ApolloConfig apolloConfig) {
        Properties result = propertiesFactory.getPropertiesInstance();
        result.putAll(apolloConfig.getConfigurations());
        return result; // {student={"name":"zhoufudn","age":20}, name=zhoufudun, keyName=key, key=keyName}
    }

    // com.ctrip.framework.apollo.internals.RemoteConfigRepository#loadApolloConfig
    private ApolloConfig loadApolloConfig() {
        // 限流
        if (!m_loadConfigRateLimiter.tryAcquire(5, TimeUnit.SECONDS)) {
            try {
                // 如果被限流则sleep 5s
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
            }
        }
        String appId = m_configUtil.getAppId();
        String cluster = m_configUtil.getCluster();
        String dataCenter = m_configUtil.getDataCenter();
        String secret = m_configUtil.getAccessKeySecret();
        Tracer.logEvent("Apollo.Client.ConfigMeta", STRING_JOINER.join(appId, cluster, m_namespace));
        //计算重试次数
        int maxRetries = m_configNeedForceRefresh.get() ? 2 : 1;
        long onErrorSleepTime = 0; // 0 means no sleep
        Throwable exception = null;

        //获得所有的配置中心的地址
        List<ServiceDTO> configServices = getConfigServices();
        String url = null;
        //循环读取配置重试次数直到成功 每一次都会循环所有的ServiceDTO数组
        retryLoopLabel:
        for (int i = 0; i < maxRetries; i++) {
            List<ServiceDTO> randomConfigServices = Lists.newLinkedList(configServices);
            // 随机所有的Config Service 的地址
            Collections.shuffle(randomConfigServices);
            // 优先访问通知配置变更的Config Service的地址 并且获取到时,需要置空,避免重复优先访问
            if (m_longPollServiceDto.get() != null) {
                randomConfigServices.add(0, m_longPollServiceDto.getAndSet(null));
            }

            //循环所有的Apollo Server的地址
            for (ServiceDTO configService : randomConfigServices) {
                if (onErrorSleepTime > 0) {
                    logger.warn(
                            "Load config failed, will retry in {} {}. appId: {}, cluster: {}, namespaces: {}",
                            onErrorSleepTime, m_configUtil.getOnErrorRetryIntervalTimeUnit(), appId, cluster, m_namespace);

                    try {
                        m_configUtil.getOnErrorRetryIntervalTimeUnit().sleep(onErrorSleepTime);
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
                // http://192.168.254.1:8080/configs/fix-server/default/application?ip=192.168.254.1&messages=%7B%22details%22%3A%7B%22fix-server%2Bdefault%2Bapplication%22%3A5%7D%7D&releaseKey=20240914225607-94c9c979ff238727
                // 组装查询配置的地址
                url = assembleQueryConfigUrl(configService.getHomepageUrl(), appId, cluster, m_namespace,
                        dataCenter, m_remoteMessages.get(), m_configCache.get());

                logger.debug("Loading config from {}", url);

                //创建HttpRequest对象
                HttpRequest request = new HttpRequest(url);
                if (!StringUtils.isBlank(secret)) {
                    Map<String, String> headers = Signature.buildHttpHeaders(url, appId, secret);
                    request.setHeaders(headers);
                }

                Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "queryConfig");
                transaction.addData("Url", url);
                try {
                    // 发起请求,返回HttpResponse对象：只会回去当前namespace的配置
                    HttpResponse<ApolloConfig> response = m_httpClient.doGet(request, ApolloConfig.class);
                    // 设置是否强制拉取缓存的标记为false
                    m_configNeedForceRefresh.set(false);
                    // 标记成功【重置一下时间】
                    m_loadConfigFailSchedulePolicy.success();

                    transaction.addData("StatusCode", response.getStatusCode());
                    transaction.setStatus(Transaction.SUCCESS);

                    if (response.getStatusCode() == 304) {
                        logger.debug("Config server responds with 304 HTTP status code.");
                        // 无新的配置, 直接返回缓存的 ApolloConfig 对象
                        return m_configCache.get();
                    }

                    // 有新的配置,进行返回新的ApolloConfig对象
                    ApolloConfig result = response.getBody(); // 远程最新配置：ApolloConfig{appId='fix-server', cluster='default', namespaceName='application', configurations={name=zhoufudun, keyName=key, student={"name":"zhoufudn","age":20}, key=keyName}, releaseKey='20240919002213-94c9c979ff172aba'}

                    logger.debug("Loaded config for {}: {}", m_namespace, result);

                    return result;
                } catch (ApolloConfigStatusCodeException ex) {
                    ApolloConfigStatusCodeException statusCodeException = ex;
                    //config not found
                    if (ex.getStatusCode() == 404) {
                        String message = String.format(
                                "Could not find config for namespace - appId: %s, cluster: %s, namespace: %s, " +
                                        "please check whether the configs are released in Apollo!",
                                appId, cluster, m_namespace);
                        statusCodeException = new ApolloConfigStatusCodeException(ex.getStatusCode(),
                                message);
                    }
                    Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(statusCodeException));
                    transaction.setStatus(statusCodeException);
                    exception = statusCodeException;
                    if (ex.getStatusCode() == 404) {
                        break retryLoopLabel;
                    }
                } catch (Throwable ex) {
                    Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
                    transaction.setStatus(ex);
                    exception = ex;
                } finally {
                    transaction.complete();
                }

                // if force refresh, do normal sleep, if normal config load, do exponential sleep
                onErrorSleepTime = m_configNeedForceRefresh.get() ? m_configUtil.getOnErrorRetryInterval() :
                        m_loadConfigFailSchedulePolicy.fail();
            }

        }
        String message = String.format(
                "Load Apollo Config failed - appId: %s, cluster: %s, namespace: %s, url: %s",
                appId, cluster, m_namespace, url);
        throw new ApolloConfigException(message, exception);
    }

    String assembleQueryConfigUrl(String uri, String appId, String cluster, String namespace,
                                  String dataCenter, ApolloNotificationMessages remoteMessages, ApolloConfig previousConfig) {

        String path = "configs/%s/%s/%s";
        List<String> pathParams =
                Lists.newArrayList(pathEscaper.escape(appId), pathEscaper.escape(cluster),
                        pathEscaper.escape(namespace));
        Map<String, String> queryParams = Maps.newHashMap();

        if (previousConfig != null) {
            queryParams.put("releaseKey", queryParamEscaper.escape(previousConfig.getReleaseKey()));
        }

        if (!Strings.isNullOrEmpty(dataCenter)) {
            queryParams.put("dataCenter", queryParamEscaper.escape(dataCenter));
        }

        String localIp = m_configUtil.getLocalIp();
        if (!Strings.isNullOrEmpty(localIp)) {
            queryParams.put("ip", queryParamEscaper.escape(localIp));
        }

        String label = m_configUtil.getApolloLabel();
        if (!Strings.isNullOrEmpty(label)) {
            queryParams.put("label", queryParamEscaper.escape(label));
        }

        if (remoteMessages != null) {
            queryParams.put("messages", queryParamEscaper.escape(GSON.toJson(remoteMessages)));
        }

        String pathExpanded = String.format(path, pathParams.toArray());

        if (!queryParams.isEmpty()) {
            pathExpanded += "?" + MAP_JOINER.join(queryParams);
        }
        if (!uri.endsWith("/")) {
            uri += "/";
        }
        return uri + pathExpanded;
    }

    private void scheduleLongPollingRefresh() {
        //将自己注册到RemoteConfigLongPollService中,实现配置更新的实时通知
        //当RemoteConfigLongPollService长轮询到该RemoteConfigRepository的Namespace下的配置更新时,会回调onLongPollNotified()方法
        remoteConfigLongPollService.submit(m_namespace, this);
    }
    // longPollNotifiedServiceDto: ServiceDTO{appName='APOLLO-CONFIGSERVICE', instanceId='localhost:apollo-configservice:8080', homepageUrl='http://192.168.254.1:8080/'}  remoteMessages= fix-server+default+application -> {Long@8026} 11
    public void onLongPollNotified(ServiceDTO longPollNotifiedServiceDto, ApolloNotificationMessages remoteMessages) {
        //设置长轮询到配置更新的Config Service 下次同步配置时,优先读取该服务
        m_longPollServiceDto.set(longPollNotifiedServiceDto); // 之后客户端去服务端的时候需要从这里获取
        m_remoteMessages.set(remoteMessages); // 设置远程消息ApolloNotificationMessages，ApolloNotificationMessages包含namespace下对应的变化，为了个变化分配一个id，本地会通过这个id向配置中获取最新配置
        // 提交同步任务
        m_executorService.submit(new Runnable() {
            @Override
            public void run() {
                // 设置是否强制拉取缓存的标记为true
                m_configNeedForceRefresh.set(true);
                //尝试同步配置
                trySync();
            }
        });
    }

    private List<ServiceDTO> getConfigServices() {
        List<ServiceDTO> services = m_serviceLocator.getConfigServices();
        if (services.size() == 0) {
            throw new ApolloConfigException("No available config service");
        }

        return services;
    }
}
