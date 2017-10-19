package com.jeus.test.quebbit2.masterCode;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jeus
 */
public class QueueStopTester {

    //CONFING PARAMETER 
    private String endPointName = "TestQueue"; //name of queue
    private String host = "localhost";
    private int port = 5672;
    private String username = "guest";
    private String password = "guest";
    private String priority;
    private boolean batch = false;
    private boolean global = false;
    private int batchSize = 1; // count of batch
    private float countMsg = 1000;

    ConnectionFactory factory = new ConnectionFactory();
    protected Channel channel;
    protected Connection connection;

    public QueueStopTester() {

        //CREATE FACTORY
        factory.setHost(host);
        factory.setPort(port);
        factory.setAutomaticRecoveryEnabled(false);
        factory.setUsername(username);
        factory.setPassword(password);
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-max-priority", 7);

        //CREATE CONNECTION AND CHANNEL
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(this.endPointName, true, false, false, args);
            channel.basicQos(batchSize, global);
        } catch (IOException | TimeoutException e) {
            System.out.println(">>GETEXCEPTION " + e.getMessage());
        }
        for (int i = 0; i < countMsg; i++) {
            try {
                String msg = "*" + i + "*";
                byte[] bytes = msg.getBytes(Charset.forName("UTF-8"));
                channel.basicPublish("", endPointName, null, bytes);
                System.out.println(">>PRODUCE " + msg);
            } catch (IOException ex) {
                Logger.getLogger(QueueStopTester.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException ex) {
            Logger.getLogger(QueueStopTester.class.getName()).log(Level.SEVERE, null, ex);
        }

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                channel.basicAck(envelope.getDeliveryTag(), true);
             
            }
        };

        boolean autoAck = false; // acknowledgment is covered below
        try {
            channel.basicConsume(endPointName, autoAck, consumer);
        } catch (IOException ex) {
            Logger.getLogger(QueueStopTester.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
