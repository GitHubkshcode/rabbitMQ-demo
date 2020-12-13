package com.ksh;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class NewTask {

    private final static String QUEUE_NAME = "hello";
    private static final String DOT = " .";
    private static final int SEND_COUNT = 20;

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("47.94.23.138");
        factory.setUsername("admin");
        factory.setPassword("admin");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //Declaring a queue is idempotent - it will only be created if it doesn't exist already.
            // 保证server重启，队列还在
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            StringBuilder sb = new StringBuilder("Hello World");
            for (int i = 0; i < SEND_COUNT; i++) {
                sb.append(DOT);
                String message = sb.toString();
                //消息持久化，确保rabbitMQ宕机，消息不丢失
                AMQP.BasicProperties persistentTextPlain = MessageProperties.PERSISTENT_TEXT_PLAIN;
                channel.basicPublish("", QUEUE_NAME, persistentTextPlain, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + message + "'");
            }
        }
    }
}