package mqtt.examples;

import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * @author ebruno
 */
public class Sender {
    public static final int DEFAULT_QOPS       = 1;
    public static final String DEFAULT_SERVER  = "localhost";
    public static final String DEFAULT_PORT    = "1883";
    public static final String DEFAULT_BROKER  = "tcp://localhost:1883";
    public static final String DEFAULT_CLIENT_ID = "mqtt_sender_Java";
    public static final String DEFAULT_TOPIC = "topic1";

    public static final String RESPONSE_TOPIC = "response";

    private final MemoryPersistence persistence = new MemoryPersistence();
    private MqttClient client;
    private MqttCmdOptions options;

    static class MqttCmdOptions {
        String server;
        String port;
        String broker;
        String clientId;
        String topic;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage");
            System.exit(1);
        }
        MqttCmdOptions options = parseArgs(args);

        try {
            Sender sender = new Sender(options);
            sender.connect();
            CountDownLatch cleanShutdown = sender.startResponseListener();
            sender.sendMessages(100);

            Thread.sleep(1000);

            cleanShutdown.countDown();

            sender.disconnect();
            System.exit(0);
        } catch ( Exception e ) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static MqttCmdOptions parseArgs(String[] args) {
        MqttCmdOptions out = new MqttCmdOptions();

        // FIXME This seems to be non-configurable ????
        out.clientId = DEFAULT_CLIENT_ID;

        out.server = DEFAULT_SERVER;
        out.port = DEFAULT_PORT;
        out.broker = DEFAULT_BROKER;

        if ( args.length == 1) {
            // See if only a topic name was provided on the command line - i.e. local testing with defaults
            out.topic = args[0];
            return out;
        } else {
            // Otherwise, do a full parse
            for ( int i = 0; i < args.length; i = i + 1 ) {
                switch ( args[i] ) {
                    case "server":
                        out.server = args[++i];
                        break;
                    case "port":
                        out.port = args[++i];
                        break;
                    case "topic":
                        out.topic = args[++i];
                        break;
                }
            }
            out.broker = "tcp://"+ out.server +":"+ out.port;
            return out;
        }
    }

    public Sender(MqttCmdOptions options) {
        this.options = options;
    }
    
    public void connect() throws MqttException {
        try {
            System.out.println("Connecting to MQTT broker: "+ options.broker);

            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(false);

            client = new MqttClient(options.broker, options.clientId, persistence);
            client.connect(connOpts);

            System.out.println("Connected");
        }
        catch ( MqttException me ) {
            System.err.println("Reason: "+ me.getReasonCode());
            System.err.println("Msg: "+ me.getMessage());
            System.err.println("Loc: "+ me.getLocalizedMessage());
            System.err.println("Cause: "+ me.getCause());
            System.err.println("Exception: "+ me);

            throw me;
        }
    }
    
    public void disconnect() throws MqttException {
        if ( client.isConnected() ) {
            client.disconnect();
        }
        System.out.println("Disconnected");
    }

    /*
     * Create and start response subscriber and return a shutdown latch that
     * the background thread can be signalled on
     */
    public CountDownLatch startResponseListener() {
        CountDownLatch latch = new CountDownLatch(1);

        ResponseSubscriber responseSub =
                new ResponseSubscriber(RESPONSE_TOPIC, latch);

        Thread responseThread = new Thread(responseSub);
        responseThread.start();

        return latch;
    }
    
    public void sendMessages(int delay) throws MqttException {
        try {
            System.out.println("Publishing to topic " + DEFAULT_TOPIC);
            
            MqttMessage message = new MqttMessage();
            
            for ( int i = 0; i < 100; i = i + 1 ) {
                int messageId = i + 1;
                String content = "Message " + messageId;
                message.setQos(DEFAULT_QOPS);
                message.setPayload(content.getBytes());
                message.setRetained(false);
                
                MqttProperties props = new MqttProperties();
                props.setResponseTopic(RESPONSE_TOPIC);
                props.setCorrelationData( (""+messageId).getBytes() );
                message.setProperties(props);
                
                System.out.println("Sending to " + DEFAULT_TOPIC + ": '"+ message.toString() + "'");
                client.publish(DEFAULT_TOPIC, message);

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    // On interruption, cancel immediately - escalate to RuntimeException
                    throw new RuntimeException(e);
                }
            }
        } catch ( MqttException me ) {
            System.err.println("Reason: "+ me.getReasonCode());
            System.err.println("Msg: "+ me.getMessage());
            System.err.println("Loc: "+ me.getLocalizedMessage());
            System.err.println("Cause: "+ me.getCause());
            System.err.println("Exception: "+ me);

            throw me;
        }
    }

    class ResponseSubscriber implements Runnable, IMqttMessageListener {
        private final String topic;
        private final CountDownLatch cleanShutdown;

        public ResponseSubscriber(String topic, CountDownLatch cleanShutdown) {
            this.topic = topic;
            this.cleanShutdown = cleanShutdown;
        }

        @Override
        public void run() {
            MqttSubscription sub = new MqttSubscription(topic, 0);
            try {
                IMqttToken token = client.subscribe(
                                    new MqttSubscription[] { sub },
                                    new IMqttMessageListener[] { this });

                // Await shutdown
                cleanShutdown.await();

                client.unsubscribe(topic);
            } catch (InterruptedException | MqttException e) {
                e.printStackTrace();
            }

            System.out.println("ResponseSubscriber Thread terminating");
        }

        @Override
        public void messageArrived(String topic, MqttMessage mm) {
            MqttProperties props = mm.getProperties();
	        String corrData = new String(props.getCorrelationData(), StandardCharsets.UTF_8);
	        System.out.println("Response received on " + topic + ": " + mm.toString() + ", Correlation data: " + corrData);
        }
    }
}
