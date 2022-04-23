package plucinski.main;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Supplier {

    public static void main(String[] argv) throws Exception {
        System.out.println("Supplier start args: " + Arrays.toString(argv));

        // init
        String inputExchangeName = "suppliers";
        String outputExchangeName = "clients";
        String adminExchangeName = "admin";
        String userExchangeName = "users";

        Scanner keyboard = new Scanner(System.in);
        String supplierName = keyboard.nextLine();
        List<Equipment> equipmentStorage = Arrays.stream(argv).map(Equipment::valueOf).toList();


        // info
        if(equipmentStorage.size()>0)
            System.out.println("Sprzedajesz " + equipmentStorage);
        else
            System.out.println("Nic nie sprzedajesz");


        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(1);

        // exchange
        channel.exchangeDeclare(inputExchangeName, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(outputExchangeName, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(userExchangeName, BuiltinExchangeType.TOPIC);

        // queue & bind
        String adminQueue = channel.queueDeclare(adminExchangeName, false, false, false, null).getQueue();
        String userQueue = channel.queueDeclare("user_"+supplierName, false, false, false, null).getQueue();

        channel.queueBind(userQueue, userExchangeName, "user.supplier");
        channel.queueBind(userQueue, userExchangeName, "user.all");
        for (Equipment equipment : equipmentStorage) {
            String queueName = channel
                    .queueDeclare(equipment.toString(), false, false, false, null).getQueue();
            channel.queueBind(queueName, inputExchangeName, equipment.toString());
        }

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String[] data = new String(delivery.getBody(), StandardCharsets.UTF_8).split(":");
            String message = data[1];
            String client = data[0];

            System.out.println(" [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "");
            String msg = "Nazwa dostawcy: " + supplierName + " towar: " + message + " POTWIERDZENIE";
            channel.basicPublish(
                    outputExchangeName,
                    client,
                    null,
                    msg.getBytes(StandardCharsets.UTF_8)
            );
            System.out.println(" [x] Sent '" + msg + "' to " + outputExchangeName);
            channel.basicPublish(
                    adminQueue,
                    "admin",
                    null,
                    ("Supplier: " + supplierName + " dostarcza: " + message + " do " + client).getBytes(StandardCharsets.UTF_8)
            );
            System.out.println(" [x] Sent '" + "Supplier: " + supplierName + " dostarcza: " + message + " do " + client + "' to " + adminQueue);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        DeliverCallback fromAdmin = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received from ADMIN: '"  + message + "");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        // start listening
        System.out.println("Waiting for messages...");
        for (Equipment equipment : equipmentStorage) {
            channel.basicConsume(equipment.toString(), false, deliverCallback, consumerTag -> { });
        }
        channel.basicConsume("user_"+supplierName, false, fromAdmin, consumerTag -> { });
    }
}
