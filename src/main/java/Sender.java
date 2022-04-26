
import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

/**
 * @author ebruno
 */
public class Sender {
    public static int qos           = 1;
    public static String server     = "localhost";
    public static String port       = "1883";
    public static String broker     = "tcp://localhost:1883";
    public static String clientId   = "mqtt_sender_Java";
    public static String topic      = "topic1";

    public static final String RESPONSE_TOPIC = "response";
    Object shutdownMonitor = new Object();

    MemoryPersistence persistence = new MemoryPersistence();
    MqttClient client;

    public static void main(String[] args) {
        // See if a topic names was provided on the command line
        if ( args.length == 1) {
            topic = args[0];
        }
        else {
            for ( int i = 0; i < args.length; i++ ) {
                switch ( args[i] ) {
                    case "server":
                        server = args[++i];
                        break;
                    case "port":
                        port = args[++i];
                        break;
                    case "topic":
                        topic = args[++i];
                        break;
                }

            }
        }

        broker = "tcp://"+server+":"+port;
 
        try {
            Sender sender = new Sender();
            
            sender.connect();
            sender.startResponseListener();
            sender.sendMessages();

            Thread.sleep(1000);
            
            sender.stopResponseListener();
            sender.disconnect();
            System.exit(0);

        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    
    public Sender() {
    }
    
    public void connect() throws MqttException {
        try {
            System.out.println("Connecting to MQTT broker: "+broker);

            MqttConnectionOptions connOpts = new MqttConnectionOptions();
            connOpts.setCleanStart(false);

            client = new MqttClient(broker, clientId, persistence);
            client.connect(connOpts);

            System.out.println("Connected");
        }
        catch ( MqttException me ) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
            throw me;
        }
    }
    
    public void disconnect() throws MqttException {
        if ( client.isConnected() ) {
            client.disconnect();
        }
        System.out.println("Disconnected");
    }
    
    public void startResponseListener() {
        try {
            // Create and start response subscriber and wait for its thread
            // to be ready by waiting on a monitor object to be signaled
            //
            ResponseSubscriber responseSub = 
                    new ResponseSubscriber(RESPONSE_TOPIC);

            responseSub.start();

            synchronized ( responseSub ) {
                responseSub.wait();
            }
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    
    public void stopResponseListener() {
        try {
            synchronized ( shutdownMonitor ) {
                shutdownMonitor.notify();
            }
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }

    public void sendMessages() {
        try {
            System.out.println("Publishing to topic " + topic );
            
            MqttMessage message = new MqttMessage();
            
            for ( int i = 0; i < 100; i++ ) {
                int messageId = i+1;
                String content = "Message " + messageId;
                message.setQos(qos);
                message.setPayload(content.getBytes());
                message.setRetained(false);
                
                MqttProperties props = new MqttProperties();
                props.setResponseTopic(RESPONSE_TOPIC);
                props.setCorrelationData( (""+messageId).getBytes() );
                message.setProperties(props);
                
                System.out.println("Sending to " + topic + ": '"+ message.toString() + "'");
                client.publish(topic, message);
                
                Thread.sleep(100);
            }
            
            synchronized ( shutdownMonitor ) {
                shutdownMonitor.notify();
            }

        } 
        catch ( MqttException me ) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
        catch ( Exception e ) { 
            e.printStackTrace();
        }
    }


    class ResponseSubscriber extends Thread implements IMqttMessageListener {
        String topic;

        public ResponseSubscriber(String topic) {
            this.topic = topic;
        }

        public void run() {
            try {
                synchronized ( shutdownMonitor ) {
                    MqttSubscription sub = new MqttSubscription(topic, 0);
                    IMqttToken token = 
                            client.subscribe(
                                    new MqttSubscription[] { sub },
                                    new IMqttMessageListener[] { this });

                    synchronized ( this ) {
                        this.notify();
                    }

                    // Don't terminate thread until signaled
                    shutdownMonitor.wait();
                }

                client.unsubscribe(topic);
            }
            catch ( Exception e ) {
                e.printStackTrace();
            }
            
            System.out.println("ResponseSubscriber Thread terminating");
        }

        @Override
        public void messageArrived(String topic, MqttMessage mm) throws Exception {
            MqttProperties props = mm.getProperties();
	    String corrData = new String(props.getCorrelationData());
	    System.out.println("Response received on " + topic + ": " + mm.toString() + ", Correlation data: " + corrData); 
        }
    }
}
