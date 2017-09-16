# RocketMQ快速上手

@(RocketMQ)[rocketmq-spring-boot-starter]


## 消费模式
- **集群消费CLUSTERING**：一个Consumer Group中的各个Consumer实例分摊去消费消息，即一条消息只会投递到一个Consumer Group下面的一个实例，每个Consumer是平均分摊Message Queue的去做拉取消费。
- **广播消费BROADCASTING**：消息将对一个Consumer Group下的各个Consumer实例都投递一遍。即即使这些 Consumer 属于同一个Consumer Group，消息也会被Consumer Group 中的每个Consumer都消费一次，一个消费组下的每个消费者实例都获取到了topic下面的每个Message Queue去拉取消费。

## 消息发送模式

- **同步阻塞**：消息到达broker之后才返回，对应synSend。
- **异步回调**：消息发送结束后触发回调，对应asynSend。
- **发送即忘**：只管发送，不管是否到达broker，对应sendOneWay。

## 准备工作

- **创建Topic**:  可以在RocketMQ 运维管理平台上创建，这里不详细赘述


## 快速体验消息生产、消费

- **pom中引入rocketmq-spring-boot-starter依赖**:
这里针对我们公司的实际情况（环境比较多，不可能每个环境都部署一套rocketmq服务）做了一些定制：
可以在配置文件中配置消息生产者的topic，配置消息消费者的consumerGroup，topic。
一般一个应用只需要一个生产者

```xml
<dependency>
	<groupId>com.maihaoche</groupId>
	<artifactId>spring-boot-starter-rocketmq</artifactId>
	<version>qh-0.0.4</version>
</dependency>
```

- **application.yml里增加rocketmq配置**:
 如果只有消费者只需要配置name-server-address，多个nameserv地址逗号分隔
 如果项目中只有消费者，不需要生产者，则不需要配置producer-group
```xml
rocketmq:
  name-server-address: 192.168.59.244:9876
  producer-group: PID_DEMO
```

- **在springboot应用主入口添加@EnableMQConfiguration注解开启自动装配**:
```java
@EnableMQConfiguration
@SpringBootApplication
public class RmqDemoApplication {
	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(RmqDemoApplication.class, args);
	}
}
```

- **创建消息生产者**:
```java
@Slf4j
//定义生产者默认将消息投放到哪个Topic，以及定义默认的tag(一般用于区分同一个业务，如订单业务对于业务订单状态变更时刻，会产生不同的消息类型，如订单创建、订单关闭、订单付款、订单发货)
@MQProducer(topic = "TP_DEMO",tag = "A")
public class DemoProducer extends AbstractMQProducer{

}
```
如果想要将topic配置在配置文件中
```java
@Slf4j
@MQProducer
public class DemoProducer extends AbstractMQProducer{

    @Value("${rocketmq.demoProducer.topic}")
    private String topic;

}
```
其中@Value("${rocketmq.demoProducer.topic}")对应application.yml里的如下配置：
```yaml
rocketmq:
   ...
  demoProducer:
    topic: TP_DEMO
```


发送消息
```java
 //注入消息生产者
 @Autowired
 private DemoProducer demoProducer;

 for(int i = 0 ; i < 10 ; i++) {
     Demo demo = new Demo("jaychang"+i, "using rocketmq "+i);
     //若不填topic参数，则默认使用DemoProducer注解@MQProducer的topic值
     demoProducer.synSend("A",demo);
     //demoProducer.synSend("TP_DEMO", "A", demo);
 }
```



- **创建消息消费者**:

```java
@MQConsumer(consumerGroup = "CONSUMER_DEMO_A",topic = "TP_DEMO",tag = "A")
public class DemoConsumerA extends AbstractMQPushConsumer<Demo>{
    @Override
    public boolean processWithKey(String messageKey, Demo message) {
        System.out.println("DemoConsumerA.processWithKey "+"messageKey=【"+messageKey+"】,message=【"+message+"】");
        return true;
    }
}
```

如果想要在配置文件中配置消息消费者的topic及consumerGroup可以按照以下写法：
```
@MQConsumer(tag = "A")
public class DemoConsumerA extends AbstractMQPushConsumer<Demo>{

    @Value("${rocketmq.demoConsumerA.topic}")
    private String topic;

    @Value("${rocketmq.demoConsumerA.consumerGroup}")
    private String consumerGroup;

    @Override
    public boolean processWithKey(String messageKey, Demo message) {
        System.out.println("DemoConsumerA.processWithKey "+"messageKey=【"+messageKey+"】,message=【"+message+"】");
        try {
            //模拟业务处理时间
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }
}
```
其中@Value("${rocketmq.demoConsumerA.topic}")与@Value("${rocketmq.demoConsumerA.consumerGroup}")对应applicaiton.yml里的如下配置
```yaml
rocketmq:
  ...

  demoConsumerA:
    topic: TP_DEMO
    consumerGroup: CONSUMER_DEMO_A
```


## 无序、顺序消息 消费的注意点

### 无序

上述快速体验里已经有介绍了，默认不设置就是无需消息。

### 有序
有序消息需要生产者，消费者一起配合，生产者要保证每次消息都要投递到broker的同一个队列里，消费者需要设置

最简单的方式：创建Topic的时候只选择包含一个queue。
如果要增加吞吐量：发送消息的时候需要选择queue来实现部分有序，可以1个tag用1个queue。

见如下代码：
```java
public class SelectMessageQueueByHash implements MessageQueueSelector {

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int value = arg.hashCode();
        if (value < 0) {
            value = Math.abs(value);
        }

        value = value % mqs.size();
        return mqs.get(value);
    }
}
```
最后一个用于MessageQueueSelector去筛选出，将消息投递到哪个queue，这里需要保证这个值是固定的，
可以根据新建Topic时候，确定有几个queue,来指定这个参数值，一般可以用"0"
```java
Demo demo = new Demo("jaychang"+i, "using rocketmq "+i);

demoProducer.synSendOrderly("TP_DEMO","O",demo,"0");
```

消息消费者需要配置consumeMode 为ORDERLY
```java
@MQConsumer(consumerGroup = "CONSUMER_FOR_ORDERLY_DEMO",topic = "TP_DEMO",tag = "O",consumeMode = "ORDERLY")
```
运行demo程序，http://127.0.0.1:8888/send31

观察控制台如下，可以发现消息是严格有序的

```java
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang0】,message=【Demo(name=jaychang0, doSomething=using rocketmq 0)】
2017-09-15 16:53:37.558  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB7FC190001, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang1】,message=【Demo(name=jaychang1, doSomething=using rocketmq 1)】
2017-09-15 16:53:37.608  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB7FD8A0002, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang2】,message=【Demo(name=jaychang2, doSomething=using rocketmq 2)】
2017-09-15 16:53:37.659  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB7FE3E0003, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang3】,message=【Demo(name=jaychang3, doSomething=using rocketmq 3)】
2017-09-15 16:53:37.709  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB7FED20004, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang4】,message=【Demo(name=jaychang4, doSomething=using rocketmq 4)】
2017-09-15 16:53:37.759  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB7FF690005, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang5】,message=【Demo(name=jaychang5, doSomething=using rocketmq 5)】
2017-09-15 16:53:37.810  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB800030006, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang6】,message=【Demo(name=jaychang6, doSomething=using rocketmq 6)】
2017-09-15 16:53:37.860  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB800A30007, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang7】,message=【Demo(name=jaychang7, doSomething=using rocketmq 7)】
2017-09-15 16:53:37.911  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB801420008, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang8】,message=【Demo(name=jaychang8, doSomething=using rocketmq 8)】
2017-09-15 16:53:37.961  INFO 15460 --- [MessageThread_1] c.m.s.mq.base.AbstractMQPushConsumer     : receive msgId: C0A83B603C6418B4AAC24BB801E20009, tags : O
DemoForOrderlyConsumer1.processWithKey messageKey=【key_jaychang9】,message=【Demo(name=jaychang9, doSomething=using rocketmq 9)】
```

这种方式可以保证消息绝对有序，但是性能还是有些损耗，故除非业务上有需要，不然就尽量不要使用顺序消息

## 集群、广播消费的注意点

## 集群消费
默认就是集群消费的模式即messageMode="CLUSTERING"，即同一消费组下，只能有一个消费实例收到消息

## 广播消费
若要使用广播消费模式,注意设置messageMode="BROADCASTING"，消费组有多个实例启动，那么这几个实例都会收到消息
```java
@MQConsumer(consumerGroup = "CONSUMER_BROAD_CAST_DEMO",topic = "TP_DEMO",messageMode = "BROADCASTING",tag = "BRODCAST")
```

## Demo地址

Demo示例里包含了无序消息、顺序消息；集群消费（默认）、广播消费的例子
http://git.sxb.lol/zhangjie/rmq-demo

## 开发环境RocketMQ Console
可以用于新建Topic，查询消息消费情况，查询消息内容，验证消费者等
http://192.168.59.244:8080

## 建议的Topic、ProducerGroup,ConsumerGroup命名规范

Demo里面关于Topic、ProducerGroup，ConsumerGroup的命名其实并不规范

这里参照阿里云ONS，定一些规范

### 前缀规范
- **Topic前缀**: TP
- **ProducerGroup前缀**:PID
- **ConsumerGroup前缀**:CID

### 单词间用下划线隔开

    TP_COMPANY_INFO_SYNC 表示公司信息同步主题
    PID_COMPANY_INFO_SYNC 公司信息同步消息生产者组
    CID_COMPANY_INFO_SYNC 公司信息同步消息消费者组
