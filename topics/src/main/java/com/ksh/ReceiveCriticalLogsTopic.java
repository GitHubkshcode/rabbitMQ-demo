package com.ksh;

import com.rabbitmq.client.Channel;

import static com.ksh.Utils.consumeMessage;
import static com.ksh.Utils.getChannel;

/**
 * topic exchange：用topic指定消息的来源、用routingKey指定消息的分类，背后的处理逻辑与direct exchange类似，通过routingKey
 * 与binding key匹配，来将消息转发给所有匹配的queue;
 *
 * 例如：分别处理Linux系统中cron的ERROR日志、kern的全部日志；
 *
 * routingKey必须是以'.'分割的单词，单词最好表示消息的功能或者分类，单词数量没有限制，长度上限为255bytes；
 *
 * 与queue绑定的binding key也必须用相同的形式，有两种特殊情况，'*'可以代替一个单词，'#'可以代替0到多个单词，
 * 例如：quick.orange.rabbit(*.orange.*)、lazy.#(lazy.brown.fox);
 *
 * 某个消息的routingKey匹配到两个队列的binding key，这个消息会转发到两个队列，
 * 某个消息的routingKey匹配到某个队列的两个binding key，这个消息只会向该队列转发一次，
 * 没有匹配到的消息，将会被丢弃；
 *
 * 用topic exchange可以实现fanout exchange和direct exchange的效果，
 * 队列的binding key值为 #，队列会接收所有的消息，达到fanout exchange的效果，
 * 队列的binding key值没有使用'*'、'#'，队列只会接收完全匹配的routingKey的消息，效果同direct exchange;
 */
public class ReceiveCriticalLogsTopic {
    public static void main(String[] argv) throws Exception {
        Channel channel = getChannel();

        channel.exchangeDeclare(Constants.EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();

        String bindingKey = "*.critical";
        channel.queueBind(queueName, Constants.EXCHANGE_NAME, bindingKey);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        consumeMessage(channel, queueName);
    }
}