# mqtt_sender

To build:

```shell
mvn package
```

To run:

```shell
java -cp target/mqtt_sender-1.0-SNAPSHOT.jar mqtt.examples.Sender <options>
```

where the options are either:

```shell
server <server hostname> port <server port> topic <topic name>
```

or just the topic, for local testing

