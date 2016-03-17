package JNode;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

class Producer
{
    private final kafka.javaapi.producer.Producer kafka;
    public Producer()
    {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        String brokerList = System.getenv("BROKER_LIST");
        if(brokerList == null) {
            brokerList = "localhost:9092";
        }
        props.put("metadata.broker.list", brokerList);
        this.kafka = new kafka.javaapi.producer.Producer(new ProducerConfig(props));
    }

    public void send(String topic, String messageStr) {
        KeyedMessage msg = new KeyedMessage(topic, messageStr);
        kafka.send(msg);
    }
}
