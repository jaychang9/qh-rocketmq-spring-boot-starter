package com.maihaoche.starter.mq.base;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.util.Assert;

/**
 * Comments：RocketMQ消费抽象基类
 * Author：Jay Chang
 * Create Date：2017/9/14
 * Modified By：
 * Modified Date：
 * Why & What is modified：
 * Version：v1.0
 */
@Slf4j
public abstract class AbstractMQConsumer<T> {

    protected static Gson gson = new Gson();
    /**默认最大重试次数5次*/
    protected static final int MAX_RETRY_TIMES = 5;

    /**
     * 继承这个方法处理消息
     *
     * @param message 消息范型
     * @param messageKey 消息key
     * @param tag 消息tag
     * @return 处理结果
     */
    public abstract boolean process(String messageKey,String tag, T message);

    /**
     * 反序列化解析消息
     *
     * @param message  消息体
     * @return 序列化结果
     */
    protected T parseMessage(MessageExt message) {
        if (message == null || message.getBody() == null) {
            return null;
        }
        final Type type = this.getMessageType();
        if (type instanceof Class) {
            String messageBody = null;
            try {
                T data = gson.fromJson(messageBody = new String(message.getBody(),"utf-8"), type);
                return data;
            } catch (JsonSyntaxException e) {
                log.error("parse message json fail : {},message body:{}", e.getMessage(),messageBody);
            } catch (UnsupportedEncodingException e) {
                log.error("parse message json fail : {},message content:{}", e.getMessage());
            }
        } else {
            log.warn("Parse msg error. {}", message);
        }
        return null;
    }

    /**
     * 解析消息类型
     *
     * @return 消息类型
     */
    protected Type getMessageType() {
        Type superType = this.getClass().getGenericSuperclass();
        if (superType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) superType;
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            Assert.isTrue(actualTypeArguments.length == 1, "Number of type arguments must be 1");
            return actualTypeArguments[0];
        } else {
            // 如果没有定义泛型，解析为Object
            return Object.class;
        }
    }

    protected boolean checkReachMaxRetryTimes(MessageExt messageExt) {
        log.info("re-consume times: {}" , messageExt.getReconsumeTimes());
        //大于最大重试次数则记录失败日志，并返回ConsumeOrderlyStatus.SUCCESS
        if(messageExt.getReconsumeTimes() >= MAX_RETRY_TIMES){
            log.error("Consumer reach the maximum number of retries,please process by manual work,msgId:{},msgKey:{},tags:{}",messageExt.getMsgId(),messageExt.getKeys(),messageExt.getTags());
            return true;
        }
        return false;
    }
}
