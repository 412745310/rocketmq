package com.chelsea.rocketmq;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class ConsumerMain {
	/**
	 * 当前例子是PushConsumer用法，使用方式给用户感觉是消息从RocketMQ服务器推到了应用客户端。<br>
	 * 但是实际PushConsumer内部是使用长轮询Pull方式从MetaQ服务器拉消息，然后再回调用户Listener方法<br>
	 */
	public static void main(String[] args) throws InterruptedException,
			MQClientException {
		/**
		 * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
		 * 注意：ConsumerGroupName需要由应用来保证唯一
		 */
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				"ConsumerGroupName");
		consumer.setNamesrvAddr("127.0.0.1:9876");
		consumer.setInstanceName("Consumber");

		/**
		 * 订阅指定topic下tags分别等于TagA或TagC或TagD
		 */
		consumer.subscribe("TopicTest2", "TagA || TagB || agC || TagD");
		/**
		 * 订阅指定topic下所有消息<br>
		 * 注意：一个consumer对象可以订阅多个topic
		 */
		//consumer.subscribe("TopicTest2", "*");
		// MessageModel.CLUSTERING 集群消费 :只能有一个消费者消费一次
		// MessageModel.BROADCASTING 广播模式 :所有的消费者都会消费一次
		consumer.setMessageModel(MessageModel.CLUSTERING);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		// 设置一次消费批量大小
		consumer.setConsumeMessageBatchMaxSize(1);
		// 设置并行消费最小线程数
		consumer.setConsumeThreadMin(5);
		// 设置并行消费最大线程数
		consumer.setConsumeThreadMax(20);

		consumer.registerMessageListener(new MessageListenerConcurrently() {

			public ConsumeConcurrentlyStatus consumeMessage(
					List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

//				System.out.println(Thread.currentThread().getName()
//						+ " Receive New Messages: " + msgs.size());

				MessageExt msg = msgs.get(0);
				if (msg.getTopic().equals("TopicTest2")) {
					// 执行TopicTest1的消费逻辑
					if (msg.getTags() != null && msg.getTags().equals("TagA")) {
						// 执行TagA的消费
						System.out.println(new String(msg.getBody()));
					} else if (msg.getTags() != null
							&& msg.getTags().equals("TagB")) {
						// 执行TagD的消费
						System.out.println(new String(msg.getBody()));
					} else if (msg.getTags() != null
							&& msg.getTags().equals("TagC")) {
						// 执行TagC的消费
						System.out.println(new String(msg.getBody()));
					} else if (msg.getTags() != null
							&& msg.getTags().equals("TagD")) {
						// 执行TagD的消费
						System.out.println(new String(msg.getBody()));
					}
				} 
//				else if (msg.getTopic().equals("TopicTest2")) {
//					System.out.println(new String(msg.getBody()));
//				}

				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

			}
		});

		/**
		 * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
		 */
		consumer.start();

		System.out.println("ConsumerStarted.");
	}

}
