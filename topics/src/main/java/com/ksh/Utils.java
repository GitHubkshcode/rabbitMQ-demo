package com.ksh;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 *@author kongsh
 *@date 2020/12/14 下午2:00
 */
class Utils {
    static Channel getChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = getConnectionFactory();

        Connection connection = factory.newConnection();
        return connection.createChannel();
    }

    static ConnectionFactory getConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("47.94.23.138");
        factory.setUsername("admin");
        factory.setPassword("admin");
        return factory;
    }

    static void consumeMessage(Channel channel, String queueName) throws IOException {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
