package com.maihaoche.starter.mq.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.Set;

/**
 * Created by yipin on 2017/6/27.
 * RocketMQ的消费者(Push模式)处理消息的接口
 */
@Slf4j
public abstract class AbstractMQPullConsumer<T> extends AbstractMQConsumer<T>{

    public AbstractMQPullConsumer() {
    }

    private String topic;

    private DefaultMQPullConsumer consumer;

    public DefaultMQPullConsumer getConsumer() {
        return consumer;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumer(DefaultMQPullConsumer consumer) {
        this.consumer = consumer;
    }

    public void startInner() {
        new Thread(() -> {
            try {
                while(true) {
                    Set<MessageQueue> mqs = consumer.fetchMessageQueuesInBalance(topic);
                    try {
                        for (MessageQueue mq : mqs) {
                            SINGLE_MQ:
                            while (true) {
                                try {//阻塞的拉去消息，中止时间默认20s
                                    long offset = consumer.fetchConsumeOffset(mq, false);
                                    offset = offset < 0 ? 0 : offset;
                                    PullResult pullResult = consumer.pull(mq, null, offset, 10);
                                    switch (pullResult.getPullStatus()) {
                                        case FOUND://pullSataus
                                            dealMessage(pullResult.getMsgFoundList());
                                            break;
                                        case NO_MATCHED_MSG:
                                            break;
                                        case NO_NEW_MSG:
                                            break SINGLE_MQ;
                                        case OFFSET_ILLEGAL:
                                            break;
                                        default:
                                            break;
                                    }
                                    consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                                } catch (Exception e) {
                                    log.error("consume message fail , e : {}", e);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("start pull consumer fail, e : {}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.error("start pull consumer fail, e : {}", e.getMessage());
            }
        }).start();
    }

    /**
     * 原生dealMessage方法，可以重写此方法自定义序列化和返回消费成功的相关逻辑
     *
     * @param list 消息列表
     */
    public void dealMessage(List<MessageExt> list) {
        for(MessageExt messageExt : list) {
            if(messageExt.getReconsumeTimes() != 0) {
                //达到最大重试次数则不对消息进行消费
                if(checkReachMaxRetryTimes(messageExt)){
                    continue;
                }
            }
            log.info("receive msgId: {}, tags : {}" , messageExt.getMsgId(), messageExt.getTags());
            T t = parseMessage(messageExt);
            process(messageExt.getKeys(),messageExt.getTags(),t);
        }
    }
}
