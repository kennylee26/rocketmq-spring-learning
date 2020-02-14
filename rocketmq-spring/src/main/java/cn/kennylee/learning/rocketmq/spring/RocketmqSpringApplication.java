package cn.kennylee.learning.rocketmq.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

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
    static final String CONSUMER1_TAGS = "rocketMqConsumer1";
    static final String CONSUMER2_TAGS = "rocketMqConsumer2";

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
        AbstractMessageHandler.ListenerParams params = buildListenerParams(CONSUMER1_TAGS);
        return generateRocketMqConsumer(new AbstractMessageHandler(params) {
            @Override
            public void onMessage(MessageExt messageExt) {
                String payload = new String(messageExt.getBody(), RocketMqProducer.DEFAULT_CHARSET);
                MESSAGE_EXTS_1.add(messageExt);
                log.info("rocketMqConsumer1: consume {} ,keys {}\n>>> body -> \n {} ", messageExt.getMsgId(), messageExt.getKeys(), payload);
            }
        }, "testConsumer1");
    }

    @Bean("rocketMqConsumer2")
    public RocketMqConsumer rocketMqConsumer2() {
        AbstractMessageHandler.ListenerParams params = buildListenerParams(CONSUMER2_TAGS);
        return generateRocketMqConsumer(new AbstractMessageHandler(params) {
            @Override
            public void onMessage(MessageExt messageExt) {
                MESSAGE_EXTS_2.add(messageExt);
                log.info("rocketMqConsumer2: consume {} ,keys {}\n>>> body -> \n {} ", messageExt.getMsgId(), messageExt.getKeys(),
                        new String(messageExt.getBody(), RocketMqProducer.DEFAULT_CHARSET));
            }
        }, "testConsumer2");
    }

    private AbstractMessageHandler.ListenerParams buildListenerParams(String tags) {
        final RocketMqClientProperties.Rocketmq rocketmqConfig = rocketMqClientProperties.getRocketmq();
        return AbstractMessageHandler.ListenerParams.builder()
                .topic(rocketmqConfig.getTopic())
                .tags(tags)
                .build();
    }

    private <T extends AbstractMessageHandler> RocketMqConsumer generateRocketMqConsumer(
            @NonNull T messageHandler, @Nullable String instanceName) {
        final String groupName = rocketMqClientProperties.getRocketmq().getConsumer().getGroup();
        final String namesrvAddr = rocketMqClientProperties.getRocketmq().getNamesrvAddr();
        return new RocketMqConsumer(groupName, namesrvAddr, instanceName, messageHandler);
    }
}
