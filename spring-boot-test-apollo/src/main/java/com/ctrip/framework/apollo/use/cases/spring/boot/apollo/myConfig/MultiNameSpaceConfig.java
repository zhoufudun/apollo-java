package com.ctrip.framework.apollo.use.cases.spring.boot.apollo.myConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;


@Component
public class MultiNameSpaceConfig {

    private static final Logger log = LoggerFactory.getLogger(MultiNameSpaceConfig.class);

    @Bean
    @ConditionalOnMissingBean
    public MyConfig getMyConfig(@Value("${order.entry.namespace:OrderEntryAssignedRouteKeys}") String n1,
                                @Value("${oes_site.exec_rpt.namespace:OesSiteExecRptAssignedRouteKeys}") String n2)
            {
        return new MyConfig(n1,n2);
    }
}
