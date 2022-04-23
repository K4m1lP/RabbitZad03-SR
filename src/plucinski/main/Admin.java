package plucinski.main;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Admin {
    public static void main(String[] argv) throws Exception {
        // init
        String adminExchangeName = "admin";
        String usersExchange = "users";

        // connection & channel
        System.out.println("connection & channel");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(adminExchangeName, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(usersExchange, BuiltinExchangeType.TOPIC);

        String queue = channel.queueDeclare(adminExchangeName, false, false, false, null).getQueue();

        channel.queueBind(queue, adminExchangeName, "admin");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "");
        };

        // start listening
        System.out.println("Waiting for messages...");
        channel.basicConsume(queue, false, deliverCallback, consumerTag -> {});

        System.out.println("Opcje:");
        System.out.println("All: ta wiadomosc dojdzie do wszystkich");
        System.out.println("Sup: ta wiadomosc dojdzie do dostawcow");
        System.out.println("Tea: ta wiadomosc dojdzie do drużyn");
        Scanner keyboard = new Scanner(System.in);
        while (true){
            String input = keyboard.nextLine();
            String key = "user.";
            if(input.startsWith("All"))
                key = key.concat("all");
            if(input.startsWith("Sup"))
                key = key.concat("supplier");
            if(input.startsWith("Tea"))
                key = key.concat("team");
            System.out.println("key: " + key);
            channel.basicPublish(
                    usersExchange,
                    key,
                    null,
                    (input.substring(4)).getBytes(StandardCharsets.UTF_8)
            );
        }


    }
}
