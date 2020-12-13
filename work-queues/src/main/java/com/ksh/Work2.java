package com.ksh;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * autoAck=true，work宕机会丢失消息，与 {@link WorkManualAck}区别看
 */
public class Work2 {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("47.94.23.138");
        factory.setUsername("admin");
        factory.setPassword("admin");
        //这里不能用try-with-resource，因为需要保证消费者异步监听消息到达时，connection+channel有效
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        //次操作幂等，确保消费前队列存在
        //确保rabbitMQ重启后队列还在
        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        //多条消息会遍历输出
        //mq server会异步推送消息，用DeliverCallback去缓存消息，知道我们准备消费消息
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [" + Work2.class.getName() + "] Received '" + message + "'");
            try {
                doWork(message);
            } catch (Exception e) {
                System.out.println("...." + e.getMessage());
            } finally {
                System.out.println(" [" + Work2.class.getName() + "] Done");
            }
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                TimeUnit.MILLISECONDS.sleep(1000 * 1000);
            }
        }
    }
}