package com.chelsea.rocketmq;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;

/**
 * 实现普通消息
 * 
 * @author Administrator
 *
 */
public class ProducerMain {

	public static void main(String[] args) throws Exception{
		/**
		 * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例<br>
		 * 注意：ProducerGroupName需要由应用来保证唯一<br>
		 * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键，
		 * 因为服务器会回查这个Group下的任意一个Producer
		 */
		final DefaultMQProducer producer = new DefaultMQProducer(
				"ProducerGroupName");
		producer.setNamesrvAddr("10.1.4.220:19875;10.1.4.220:19876");
		producer.setInstanceName("Producer");

		/**
		 * Producer对象在使用之前必须要调用start初始化，初始化一次即可<br>
		 * 注意：切记不可以在每次发送消息时，都调用start方法
		 */
		producer.start();
		int i = 0;
        while (true) {
            String message = "aaa" + i;
            i++;
            Message msg = new Message();
            msg.setTopic("AllocateTopic");
            msg.setBody(message.getBytes("utf-8"));
            SendResult sendResult = producer.send(msg);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                System.out.println("发送消息：" + message);
            }
            Thread.sleep(1000);
        }

    }
}
