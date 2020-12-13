package com.ksh;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

/**
 * 通过routingKey控制来达到消费部分消息的目的，比如：将ERROR日志写到日志文件、将全量日志输出到控制台
 * 消息会发到与其routingKey相同的队列中
 * exchange type = fanout，会忽略routingKey
 * 一个队列可以绑定两个routingKey，该队列将收到绑定的两个routingKey的消息
 * 一个routingKey可以绑定到两个队列，该routingKey的消息会广播到绑定该routingKey的两个队列
 */
public class ReceiveErrorLogsDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("47.94.23.138");
        factory.setUsername("admin");
        factory.setPassword("admin");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();

        String[] logLevels = {"error"};
        for (String logLevel : logLevels) {
            String routingKey = logLevel + "LogKey";
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}