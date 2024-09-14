package com.ctrip.framework.apollo.use.cases.spring.boot.apollo.controller;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.use.cases.spring.boot.apollo.JsonResult;
import com.ctrip.framework.apollo.use.cases.spring.boot.apollo.myConfig.MyConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * AssignedRoutingKeyController
 */
@RestController
@RequestMapping("/AssignedRouteKey")
public class AssignedRoutingKeyController {

    private Environment environment;

    private MyConfig myConfig;

    private static final String PUBLISH_PREFIX = "rabbitmq.publish.";

    @Value("${order.entry.namespace:OrderEntryAssignedRouteKeys}")
    private String orderEntry2Namespace;

    @Value("${oes_site.exec_rpt.namespace:OesSiteExecRptAssignedRouteKeys}")
    private String oesExecRpt2Namespace;

    private static final String ORDER_ENTRY_EXCHANGE = "order.entry";
    private static final String OES_SITE_EXEC_RPT_EXCHANGE = "oes_site.exec_rpt";

    private static final Map<String, Config> configMap = new HashMap<>();

    @Autowired
    public AssignedRoutingKeyController(MyConfig myConfig, Environment environment) {
        this.myConfig = myConfig;
        this.environment = environment;
    }

    @PostConstruct
    public void init() {
        configMap.put(ORDER_ENTRY_EXCHANGE, myConfig.getConfigByNamespace(orderEntry2Namespace));
        configMap.put(OES_SITE_EXEC_RPT_EXCHANGE, myConfig.getConfigByNamespace(oesExecRpt2Namespace));
    }

    /**
     * 根据账户ID和交换机名称获取路由key
     *
     * @param id       账户ID
     * @param exchange 交换机目前有：order.entry、oes_site.exec_rpt
     */
    @GetMapping("/{id}/{exchange}")
    public JsonResult getKeyByAccount(@PathVariable Long id, @PathVariable String exchange) {
        Config config = configMap.get(exchange);
        if (config != null) {
            String property = config.getProperty(PUBLISH_PREFIX + exchange + "." + id, null);
            if (property != null && !property.isEmpty()) {
                return JsonResult.ok(property, "success");
            }
        }
        return JsonResult.fail("", "");
    }

    /**
     * 两种方法都可以
     *
     * @param id
     * @param exchange
     * @return
     */
    @GetMapping("/env/{id}/{exchange}")
    public JsonResult getKeyByAccountFromEnv(@PathVariable Long id, @PathVariable String exchange) {
        String key = PUBLISH_PREFIX + exchange + "." + id;
        String property = environment.getProperty(key);
        if (property != null && !property.isEmpty()) {
            return JsonResult.ok(property, "success");
        }
        return JsonResult.fail("", "");
    }

    @GetMapping("/All")
    public JsonResult getAllKey() {
        // 遍历map获取所有的key和value
        Map<String, String> allKeyValueMap = myConfig.getAllKeyValueMap();
        return JsonResult.ok(allKeyValueMap);
    }

}