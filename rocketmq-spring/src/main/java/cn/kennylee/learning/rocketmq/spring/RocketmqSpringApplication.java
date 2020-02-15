package cn.kennylee.learning.rocketmq.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.NonNull;

import javax.annotation.Resource;
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

    static final Set<MessageExt> MESSAGE_EXTS_1 = new HashSet<>();
    static final Set<MessageExt> MESSAGE_EXTS_2 = new HashSet<>();

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
    public RocketMqConsumer rocketMqConsumer1() {
        AbstractMessageHandler.ListenerParams params = AbstractMessageHandler.ListenerParams.builder()
                .topic(rocketMqClientProperties.getRocketmq().getTopic())
                .tags(TAGS_1)
                .build();
        ;
        return generateRocketMqConsumer(new AbstractMessageHandler(params) {
            @Override
            public void onMessage(MessageExt messageExt) {
                String payload = new String(messageExt.getBody(), RocketMqProducer.DEFAULT_CHARSET);
                MESSAGE_EXTS_1.add(messageExt);
                log.info("rocketMqConsumer1: consume {} ,keys {}\n>>> body -> \n {} ", messageExt.getMsgId(), messageExt.getKeys(), payload);
            }
        }, CONSUMER_GROUP_NAME_1);
    }

    @Bean("rocketMqConsumer2")
    public RocketMqConsumer rocketMqConsumer2() {
        AbstractMessageHandler.ListenerParams params = AbstractMessageHandler.ListenerParams.builder()
                .topic(rocketMqClientProperties.getRocketmq().getTopic())
                .tags(TAGS_2)
                .build();
        ;
        return generateRocketMqConsumer(new AbstractMessageHandler(params) {
            @Override
            public void onMessage(MessageExt messageExt) {
                MESSAGE_EXTS_2.add(messageExt);
                log.info("rocketMqConsumer2: consume {} ,keys {}\n>>> body -> \n {} ", messageExt.getMsgId(), messageExt.getKeys(),
                        new String(messageExt.getBody(), RocketMqProducer.DEFAULT_CHARSET));
            }
        }, CONSUMER_GROUP_NAME_2);
    }

    private <T extends AbstractMessageHandler> RocketMqConsumer generateRocketMqConsumer(
            @NonNull T messageHandler, @NonNull String groupName) {
        final String namesrvAddr = rocketMqClientProperties.getRocketmq().getNamesrvAddr();
        return new RocketMqConsumer(groupName, namesrvAddr, null, messageHandler);
    }
}
