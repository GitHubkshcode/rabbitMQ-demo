package com.ksh;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

import static com.ksh.Utils.getConnectionFactory;

public class EmitLogTopic {
    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = getConnectionFactory();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(Constants.EXCHANGE_NAME, "topic");

            String routingKey = "kern.critical";
            String message = "A critical kernel error";

            channel.basicPublish(Constants.EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
        }
    }
}