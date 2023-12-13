package org.apache.eventmesh.tcp.demo.sink;

import static org.apache.eventmesh.common.Constants.CLOUD_EVENTS_PROTOCOL_NAME;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.client.tcp.EventMeshTCPClient;
import org.apache.eventmesh.client.tcp.EventMeshTCPClientFactory;
import org.apache.eventmesh.client.tcp.common.EventMeshCommon;
import org.apache.eventmesh.client.tcp.conf.EventMeshTCPClientConfig;
import org.apache.eventmesh.common.Constants;
import org.apache.eventmesh.common.ExampleConstants;
import org.apache.eventmesh.common.protocol.tcp.UserAgent;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.common.utils.LogUtils;
import org.apache.eventmesh.common.utils.ThreadUtils;
import org.apache.eventmesh.connector.wechat.sink.connector.WeChatSinkConnector;
import org.apache.eventmesh.openconnect.Application;
import org.apache.eventmesh.openconnect.util.ConfigUtil;
import org.apache.eventmesh.tcp.common.EventMeshTestUtils;
import org.apache.eventmesh.tcp.common.UtilsConstants;
import org.apache.eventmesh.util.Utils;
import org.apache.eventmesh.connector.wechat.config.WeChatConnectServerConfig;

/**
 * @author Wizard
 * @Type WeChatSinkDemo
 * @Desc
 * @date 2023/12/11 16:07
 * @Version 1.0
 */

@Slf4j
public class WeChatSinkDemo {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        final Properties properties = Utils.readPropertiesFile(ExampleConstants.CONFIG_FILE_NAME);
        final String eventMeshIp = properties.getProperty(ExampleConstants.EVENTMESH_IP);
        final int eventMeshTcpPort = Integer.parseInt(properties.getProperty(ExampleConstants.EVENTMESH_TCP_PORT));
        try {
            UserAgent userAgent = EventMeshTestUtils.generateClient1();
            EventMeshTCPClientConfig eventMeshTcpClientConfig = EventMeshTCPClientConfig.builder()
                .host(eventMeshIp)
                .port(eventMeshTcpPort)
                .userAgent(userAgent)
                .build();
            final EventMeshTCPClient<CloudEvent> client =
                EventMeshTCPClientFactory.createEventMeshTCPClient(eventMeshTcpClientConfig, CloudEvent.class);
            client.init();

            // String content = "{\"touser\":\"osDud6GBrU6_oDbvtQLY1p5601KI\",\"template_id\":\"Z8iQbtlGOPEsP5Np52RUx2u-DicDJAL3r3VqP_bae80\",\"url\":\"http://weixin.qq.com/download\",\"topcolor\":\"#FF0000\",\"data\":{\"User\":{\"value\":\"黄先生\",\"color\":\"#173177\"},\"Date\":{\"value\":\"06月07日 19时24分\",\"color\":\"#173177\"},\"CardNumber\":{\"value\":\"0426\",\"color\":\"#173177\"},\"Type\":{\"value\":\"消费\",\"color\":\"#173177\"},\"Money\":{\"value\":\"人民币260.00元\",\"color\":\"#173177\"},\"DeadTime\":{\"value\":\"06月07日19时24分\",\"color\":\"#173177\"},\"Left\":{\"value\":\"6504.09\",\"color\":\"#173177\"}}}";
            //String content = "{\"touser\":\"osDud6GBrU6_oDbvtQLY1p5601KI\",\"template_id\":\"_zQq4dE38OMWki4MXhp_qNKIVsWAAeROg6xpzK9iE_E\",\"url\":\"http://weixin.qq.com/download\",\"topcolor\":\"#FF0000\",\"data\":{\"User\":{\"value\":\"黄先生\",\"color\":\"#173177\"},\"Date\":{\"value\":\"06月07日 19时24分\",\"color\":\"#173177\"},\"CardNumber\":{\"value\":\"0426\",\"color\":\"#173177\"},\"Type\":{\"value\":\"消费\",\"color\":\"#173177\"},\"Money\":{\"value\":\"人民币260.00元\",\"color\":\"#173177\"},\"DeadTime\":{\"value\":\"06月07日19时24分\",\"color\":\"#173177\"},\"Left\":{\"value\":\"6504.09\",\"color\":\"#173177\"}}}";
            Map<String, Object> contentMap = new HashMap<>();
            contentMap.put("touser", "osDud6GBrU6_oDbvtQLY1p5601KI");
            contentMap.put("template_id", "_zQq4dE38OMWki4MXhp_qNKIVsWAAeROg6xpzK9iE_E");

            Map<String, Object> data = new HashMap<>();
            Map<String, Object> dataDetail = new HashMap<>();
            dataDetail.put("value", System.currentTimeMillis());
            dataDetail.put("color", "#173177");
            data.put("Id", dataDetail);
            contentMap.put("data",  data);

            CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSubject("TEST-TOPIC-WECHAT")
                .withSource(URI.create("/"))
                .withDataContentType(ExampleConstants.CLOUDEVENT_CONTENT_TYPE)
                .withType(CLOUD_EVENTS_PROTOCOL_NAME)
                .withData(Objects.requireNonNull(JsonUtils.toJSONString(contentMap)).getBytes(StandardCharsets.UTF_8))
                .withExtension(UtilsConstants.TTL, "30000")
                .build();
            LogUtils.info(log, "begin send async msg: {}", event);
            client.publish(event, EventMeshCommon.DEFAULT_TIME_OUT_MILLS);

            ThreadUtils.sleep(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("AsyncPublish failed", e);
        }

        WeChatConnectServerConfig weChatConnectServerConfig = ConfigUtil.parse(WeChatConnectServerConfig.class,
            Constants.CONNECT_SERVER_CONFIG_FILE_NAME);

        if (weChatConnectServerConfig.isSinkEnable()) {
            Application application = new Application();
            application.run(WeChatSinkConnector.class);
        }
    }

}