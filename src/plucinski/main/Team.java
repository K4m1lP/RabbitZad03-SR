package plucinski.main;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Team {
    public static void main(String[] argv) throws Exception {
        System.out.println("Team start args: " + Arrays.toString(argv));
        // init
        String outputExchangeName = "suppliers";
        String inputExchangeName = "clients";
        String adminExchangeName = "admin";
        String userExchangeName = "users";

        Scanner keyboard = new Scanner(System.in);
        String teamName = keyboard.nextLine();
        List<Equipment> orders = Arrays.stream(argv).map(Equipment::valueOf).toList();

        // info
        if(orders.size()>0)
            System.out.println("Zamawiasz " + orders);
        else
            System.out.println("Nic nie zamawiasz");


        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // exchange
        channel.exchangeDeclare(outputExchangeName, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(inputExchangeName, BuiltinExchangeType.DIRECT);

        channel.exchangeDeclare(userExchangeName, BuiltinExchangeType.TOPIC);

        // binding
        String queueName = channel.queueDeclare(teamName, false, false, false, null).getQueue();
        String adminQueue = channel.queueDeclare(adminExchangeName, false, false, false, null).getQueue();
        String userQueue = channel.queueDeclare("user_"+teamName, false, false, false, null).getQueue();

        channel.queueBind(queueName, inputExchangeName, teamName);
        channel.queueBind(adminQueue, inputExchangeName, adminExchangeName);
        channel.queueBind(userQueue, userExchangeName, "user.all");
        channel.queueBind(userQueue, userExchangeName, "user.team");


        // listening
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        DeliverCallback fromAdmin = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received from ADMIN: '"  + message + "");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        // sending
        orders.forEach(e -> {
            try {
                channel.basicPublish(
                        outputExchangeName,
                        e.toString(),
                        null,
                        (teamName+":"+e).getBytes(StandardCharsets.UTF_8)
                );
                System.out.println(" [x] Sent '" + teamName+":"+e + " to " + outputExchangeName);
                channel.basicPublish(
                        adminQueue,
                        "admin",
                        null,
                        ("Team: " + teamName+" zamawia: "+e).getBytes(StandardCharsets.UTF_8)
                );
                System.out.println(" [x] Sent 'Team: " + teamName+" zamawia: "+e + "' to " + adminQueue);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });

        // start listening
        System.out.println("Waiting for messages...");
        channel.basicConsume(teamName, false, deliverCallback, consumerTag -> { });
        channel.basicConsume("user_"+teamName, false, fromAdmin, consumerTag -> { });
    }

}
