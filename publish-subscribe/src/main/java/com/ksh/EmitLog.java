package com.ksh;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

/**
 * The fanout exchange is very simple. As you can probably guess from the name, it just broadcasts all the messages
 * it receives to all the queues it knows.
 *
 * fanout exchange:将所有收到的消息广播给所有绑定的队列
 */
public class EmitLog {

    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("47.94.23.138");
        factory.setUsername("admin");
        factory.setPassword("admin");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //必须要先申明exchange，才能将消息发布到exchange
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            String message = argv.length < 1 ? "info: Hello World!" : String.join(" ", argv);

            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}