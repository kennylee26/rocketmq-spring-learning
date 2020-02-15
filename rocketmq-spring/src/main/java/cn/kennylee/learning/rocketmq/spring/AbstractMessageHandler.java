package cn.kennylee.learning.rocketmq.spring;

import lombok.Builder;
import lombok.Data;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Objects;

/**
 * <p> Consumer的消息处理 </p>
 * <p>Created on 13/2/2020.</p>
 *
 * @author kennylee
 */
@Data
public abstract class AbstractMessageHandler<T> {
    private String topic;
    private String tags;
    private MessageModel messageModel = MessageModel.CLUSTERING;
    private ConsumeMode consumeMode = ConsumeMode.CONCURRENTLY;
    private AccessChannel accessChannel = AccessChannel.LOCAL;

    private int suspendCurrentQueueTimeMillis = 1000;
    private int delayLevelWhenNextConsume = 0;

    private AbstractMessageHandler() {
        // do nothing
    }

    public AbstractMessageHandler(ListenerParams params) {
        super();

        Assert.notNull(params.getTopic(), "Topic cant be null");
        this.topic = params.getTopic();
        if (!StringUtils.isEmpty(params.getTags())) {
            this.tags = params.getTags();
        }
        if (Objects.nonNull(params.getMessageModel())) {
            this.messageModel = params.getMessageModel();
        }
    }

    /**
     * <p>处理消息</p>
     *
     * @param message 消息内容
     */
    public abstract void onMessage(@NonNull T message);

    @Data
    @Builder
    public static class ListenerParams {
        private String topic;
        private String tags;
        private MessageModel messageModel;
        private ConsumeMode consumeMode;
    }
}
