package cn.kennylee.learning.rocketmq.spring;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * <p> 配置信息文件 </p>
 * <p>Created on 14/2/2020.</p>
 *
 * @author kennylee
 */
@Data
@ConfigurationProperties(prefix = "app")
public class RocketMqClientProperties {
    private Rocketmq rocketmq;

    @Data
    public static class Rocketmq {
        private String topic;
        private String namesrvAddr;
        private Producer producer;
    }

    @Data
    public static class Producer {
        private String group;
    }
}
