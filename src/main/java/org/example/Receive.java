package org.example;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class Receive {
    private final static boolean DURABLE = true;

    public String receiveMessage(String queueName) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(queueName, DURABLE, false, false, null);

            GetResponse response = channel.basicGet(queueName, true);
            if (response == null) {
                return null;
            }
            return new String(response.getBody(), StandardCharsets.UTF_8);
        }
    }
}