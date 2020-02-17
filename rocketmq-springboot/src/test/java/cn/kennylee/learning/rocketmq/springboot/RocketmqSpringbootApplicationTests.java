package cn.kennylee.learning.rocketmq.springboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Slf4j
@SpringBootTest
class RocketmqSpringbootApplicationTests {
    private static final long MAX_WAIT = 5000;
    private static final Set<RocketmqSpringbootApplication.OrderInfo> consumerMessageExts1 = RocketmqSpringbootApplication.MESSAGE_EXTS_1;
    private static final Set<RocketmqSpringbootApplication.OrderInfo> consumerMessageExts2 = RocketmqSpringbootApplication.MESSAGE_EXTS_2;

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Test
    void contextLoads() {
    }

    /**
     * 测试发送（不管结果）和接收。
     */
    @RepeatedTest(5)
    public void testRocketMqProducer_oneWay() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String payload = "Hello World@" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYYMMddHHmmss"));
        rocketMQTemplate.sendOneWay(generateDestination(),
                RocketmqSpringbootApplication.OrderInfo.builder().key(key).content(payload).build());

        assertMatch(key, payload);
    }

    /**
     * 测试发送并验证结果和测试接收。
     */
    @RepeatedTest(5)
    public void testRocketMqProducer_syncSend() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String payload = "Hello World@" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYYMMddHHmmss"));

        SendResult result = rocketMQTemplate.syncSend(generateDestination(),
                RocketmqSpringbootApplication.OrderInfo.builder().key(key).content(payload).build());
        Assertions.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        assertMatch(key, payload);
    }

    private static void assertMatch(String key, String payload) throws InterruptedException {
        long count = 0;
        long interval = 100;
        boolean flag = false;
        while (count < MAX_WAIT) {
            if (contains(consumerMessageExts1, key)) {
                flag = true;
                break;
            }
            Thread.sleep(interval);
            count += interval;
        }
        if (contains(consumerMessageExts1, key) && contains(consumerMessageExts2, key)) {
            throw new RuntimeException("consumerMessageExts2 must not has key: " + key);
        }

        Assertions.assertTrue(flag, "cant find key: " + key);

        RocketmqSpringbootApplication.OrderInfo receiveMessage = getByKey(consumerMessageExts1, key);
        Assertions.assertNotNull(receiveMessage);
        Assertions.assertEquals(payload, receiveMessage.getContent());
    }

    private static boolean contains(@NonNull Set<RocketmqSpringbootApplication.OrderInfo> consumerMessageContainer,
                                    @NonNull String keys) {
        return Objects.nonNull(getByKey(consumerMessageContainer, keys));
    }

    @Nullable
    private static RocketmqSpringbootApplication.OrderInfo getByKey(
            @NonNull Set<RocketmqSpringbootApplication.OrderInfo> consumerMessageContainer,
            @NonNull String keys) {
        return consumerMessageContainer.stream()
                .filter(o -> StringUtils.equals(o.getKey(), keys)
                ).findFirst().orElse(null);
    }

    /**
     * 生成tags1的Destination
     */
    private static String generateDestination() {
        return RocketmqSpringbootApplication.TOPIC + ":" + RocketmqSpringbootApplication.TAGS_1;
    }
}
