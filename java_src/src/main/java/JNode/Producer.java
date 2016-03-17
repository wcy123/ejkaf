package JNode;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

class Producer
{
    private final kafka.javaapi.producer.Producer<String,String> kafka;
    final static Logger logger = Logger.getLogger(Producer.class);
    public Producer()
    {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        String brokerList = System.getenv("BROKER_LIST");
        if(brokerList == null) {
            brokerList = "localhost:9092";
        }
        props.put("metadata.broker.list", brokerList);
        this.kafka = new kafka.javaapi.producer.Producer<String,String>(new ProducerConfig(props));
    }

    public void send(String topic, String messageStr) {
        KeyedMessage<String,String> msg;
        msg = new KeyedMessage<String,String>(topic, messageStr);
        try {
            kafka.send(msg);
        }catch( Exception e) {
            logger.error("cannot send msg " + msg, e);
            System.exit(1);
        }
    }
}
