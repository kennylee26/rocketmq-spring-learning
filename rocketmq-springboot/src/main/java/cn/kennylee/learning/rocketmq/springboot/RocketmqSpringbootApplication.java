package cn.kennylee.learning.rocketmq.springboot;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @author kennylee
 */
@SpringBootApplication
public class RocketmqSpringbootApplication {
    private static final String CONSUMER_GROUP_NAME_1 = "learning-springboot-consumer-group1";
    private static final String CONSUMER_GROUP_NAME_2 = "learning-springboot-consumer-group2";
    static final String TOPIC = "rocketmq-learning-springboot";
    static final String TAGS_1 = "unit-test-1";
    static final String TAGS_2 = "unit-test-2";

    static final Set<OrderInfo> MESSAGE_EXTS_1 = new HashSet<>();
    static final Set<OrderInfo> MESSAGE_EXTS_2 = new HashSet<>();

    public static void main(String[] args) {
        SpringApplication.run(RocketmqSpringbootApplication.class, args);
    }

    @Slf4j
    @Service
    @RocketMQMessageListener(topic = TOPIC, selectorExpression = TAGS_1, consumerGroup = CONSUMER_GROUP_NAME_1)
    public static class MyConsumer1 implements RocketMQListener<OrderInfo> {
        @Override
        public void onMessage(OrderInfo orderInfo) {
            MESSAGE_EXTS_1.add(orderInfo);
            log.info("MyConsumer1: {}", orderInfo.toString());
        }
    }

    @Slf4j
    @Service
    @RocketMQMessageListener(topic = TOPIC, selectorExpression = TAGS_2, consumerGroup = CONSUMER_GROUP_NAME_2)
    public static class MyConsumer2 implements RocketMQListener<OrderInfo> {
        @Override
        public void onMessage(OrderInfo orderInfo) {
            MESSAGE_EXTS_2.add(orderInfo);
            log.info("MyConsumer2: {}", orderInfo.toString());
        }
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
