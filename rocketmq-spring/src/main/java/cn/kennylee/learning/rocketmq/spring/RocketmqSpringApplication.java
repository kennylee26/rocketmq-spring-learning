package cn.kennylee.learning.rocketmq.spring;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.NonNull;

import javax.annotation.Resource;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @author kennylee
 */
@Slf4j
@SpringBootApplication
@EnableConfigurationProperties({RocketMqClientProperties.class})
public class RocketmqSpringApplication {
    private static final String CONSUMER_GROUP_NAME_1 = "learning-spring-consumer-group1";
    private static final String CONSUMER_GROUP_NAME_2 = "learning-spring-consumer-group2";
    static final String TAGS_1 = "unit-test-1";
    static final String TAGS_2 = "unit-test-2";

    static final Set<Object> MESSAGE_1 = new HashSet<>();
    static final Set<Object> MESSAGE_2 = new HashSet<>();

    @Resource
    private RocketMqClientProperties rocketMqClientProperties;

    public static void main(String[] args) {
        SpringApplication.run(RocketmqSpringApplication.class, args);
    }

    @Bean
    @ConditionalOnMissingBean(RocketMqProducer.class)
    public RocketMqProducer rocketMqProducer() {
        final String groupName = rocketMqClientProperties.getRocketmq().getProducer().getGroup();
        final String namesrvAddr = rocketMqClientProperties.getRocketmq().getNamesrvAddr();
        return new RocketMqProducer(groupName, namesrvAddr);
    }

    @Bean("rocketMqConsumer1")
    public RocketMqConsumer<OrderInfo> rocketMqConsumer1() {
        AbstractMessageHandler.ListenerParams params = AbstractMessageHandler.ListenerParams.builder()
                .topic(rocketMqClientProperties.getRocketmq().getTopic())
                .tags(TAGS_1)
                .build();
        ;
        return generateRocketMqConsumer(new AbstractMessageHandler<OrderInfo>(params) {
            @Override
            public void onMessage(OrderInfo message) {
                MESSAGE_1.add(message);
                log.info("rocketMqConsumer1: {} ", message.toString());
            }
        }, CONSUMER_GROUP_NAME_1);
    }

    @Bean("rocketMqConsumer2")
    public RocketMqConsumer<OrderInfo> rocketMqConsumer2() {
        AbstractMessageHandler.ListenerParams params = AbstractMessageHandler.ListenerParams.builder()
                .topic(rocketMqClientProperties.getRocketmq().getTopic())
                .tags(TAGS_2)
                .build();
        ;
        return generateRocketMqConsumer(new AbstractMessageHandler<OrderInfo>(params) {
            @Override
            public void onMessage(OrderInfo message) {
                MESSAGE_2.add(message);
                log.info("rocketMqConsumer2: {} ", message.toString());
            }
        }, CONSUMER_GROUP_NAME_2);
    }

    private <T extends AbstractMessageHandler<OrderInfo>> RocketMqConsumer<OrderInfo> generateRocketMqConsumer(
            @NonNull T messageHandler, @NonNull String groupName) {
        final String namesrvAddr = rocketMqClientProperties.getRocketmq().getNamesrvAddr();
        return new RocketMqConsumer<>(groupName, namesrvAddr, null, messageHandler);
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderInfo implements Serializable {
        private String key;
        private String content;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            OrderInfo orderInfo = (OrderInfo) o;

            return new EqualsBuilder()
                    .append(key, orderInfo.key)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(key)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                    .append("key", key)
                    .append("content", content)
                    .toString();
        }
    }
}
