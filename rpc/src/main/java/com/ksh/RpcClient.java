package com.ksh;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 *@author kongsh
 *@date 2020/12/14 下午3:24
 *
 * 1、创建connection、channel；
 * 2、call方法发送rpc请求；
 *  1）生成唯一的correlationId，将其保存在BasicProperties，consumer callback用correlationId匹配response
 *  2）创建一个独占的队列去接收、消费response
 *  3）发布消息，设置两个属性：replyTo(接收response的队列)、correlationId(匹配请求)
 *  4）创建一个容量为1的ArrayBlockingQueue，阻塞主线程，等待一个响应到达
 *  5）消费者处理消息：如果响应消息中的correlationId与监听的一致，则把响应放到ArrayBlockingQueue中
 *  6）同时main线程将ArrayBlockingQueue中的response取出
 *  7）最后返回响应
 *
 */
public class RpcClient implements AutoCloseable {
    private Connection connection;
    private Channel channel;
    private static final int REQUEST_COUNT = 10;

    private RpcClient() throws IOException, TimeoutException {
        connection = Utils.getConnection();
        channel = this.connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RpcClient fibonacciRpc = new RpcClient()) {
            for (int i = 0; i < REQUEST_COUNT; i++) {
                String iStr = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + iStr + ")");
                String response = fibonacciRpc.call(iStr);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String call(String message) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();

        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        String requestQueueName = "rpc_queue";
        channel.basicPublish("", requestQueueName, props, message.getBytes(StandardCharsets.UTF_8));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            System.out.println("consume thread name" + Thread.currentThread().getName());
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), StandardCharsets.UTF_8));
            }
        }, consumerTag -> {
        });

        String result = response.take();
        channel.basicCancel(ctag);
        return result;
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
