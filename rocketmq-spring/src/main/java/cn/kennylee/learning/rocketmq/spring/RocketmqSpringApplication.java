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
import org.apache.rocketmq.common.message.MessageExt;
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
    private static final String CONSUMER_GROUP_NAME_3 = "learning-spring-consumer-group3";
    private static final String CONSUMER_GROUP_NAME_4 = "learning-spring-consumer-group4";
    static final String TAGS_1 = "unit-test-1";
    static final String TAGS_2 = "unit-test-2";
    static final String TAGS_3 = "unit-test-3";
    static final String TAGS_4 = "unit-test-4";

    static final Set<OrderInfo> MESSAGE_1 = new HashSet<>();
    static final Set<String> MESSAGE_2 = new HashSet<>();
    static final Set<MessageExt> MESSAGE_3 = new HashSet<>();
    static final Set<MessageExt> MESSAGE_4 = new HashSet<>();

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

    /**
     * 接收实体类型<code>cn.kennylee.learning.rocketmq.spring.RocketmqSpringApplication.OrderInfo</code>的消费者
     */
    @Bean
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

    /**
     * 接收字符串类型的消费者
     */
    @Bean
    public RocketMqConsumer<String> rocketMqConsumer2() {
        AbstractMessageHandler.ListenerParams params = AbstractMessageHandler.ListenerParams.builder()
                .topic(rocketMqClientProperties.getRocketmq().getTopic())
                .tags(TAGS_2)
                .build();
        ;
        return generateRocketMqConsumer(new AbstractMessageHandler<String>(params) {
            @Override
            public void onMessage(String message) {
                MESSAGE_2.add(message);
                log.info("rocketMqConsumer2: {} ", message.toString());
            }
        }, CONSUMER_GROUP_NAME_2);
    }

    /**
     * 接收未定义返回数据类型的消费者（实际上默认类型就是MessageExt）
     */
    @Bean
    public RocketMqConsumer rocketMqConsumer3() {
        AbstractMessageHandler.ListenerParams params = AbstractMessageHandler.ListenerParams.builder()
                .topic(rocketMqClientProperties.getRocketmq().getTopic())
                .tags(TAGS_3)
                .build();
        ;
        return generateRocketMqConsumer(new AbstractMessageHandler(params) {
            @Override
            public void onMessage(Object message) {
                MESSAGE_3.add((MessageExt) message);
                log.info("rocketMqConsumer3: {} ", message.toString());
            }
        }, CONSUMER_GROUP_NAME_3);
    }

    /**
     * 接收Object数据类型的消费者（实际上也就是MessageExt）
     */
    @Bean
    public RocketMqConsumer<Object> rocketMqConsumer4() {
        AbstractMessageHandler.ListenerParams params = AbstractMessageHandler.ListenerParams.builder()
                .topic(rocketMqClientProperties.getRocketmq().getTopic())
                .tags(TAGS_4)
                .build();
        ;
        return generateRocketMqConsumer(new AbstractMessageHandler<Object>(params) {
            @Override
            public void onMessage(Object message) {
                MESSAGE_4.add((MessageExt) message);
                log.info("rocketMqConsumer4: {} ", message.toString());
            }
        }, CONSUMER_GROUP_NAME_4);
    }

    private <E, T extends AbstractMessageHandler<E>> RocketMqConsumer<E> generateRocketMqConsumer(
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
