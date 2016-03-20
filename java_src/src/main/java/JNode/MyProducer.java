package JNode;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import java.util.Properties;

class MyProducer
{
    private final Producer<byte[],byte[]> producer;
    ByteArraySerializer serializer;
    final static Logger logger = Logger.getLogger(MyProducer.class);
    public MyProducer()
    {
        Properties props = new Properties();
        String brokerList = System.getenv("BROKER_LIST");
        if(brokerList == null) {
            brokerList = "localhost:9092";
        }
        props.put("bootstrap.servers", brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // props.put(ProducerConfig.BATCH_SIZE_CONFIG, "200");
        this.serializer = new ByteArraySerializer();
        Serializer<byte[]> s = this.serializer;
        this.producer = new KafkaProducer<byte[],byte[]>(props,s,s);
    }

    public void send(String topic, byte[] messageStr, Callback callback) {
        ProducerRecord<byte[],byte[]> msg;
        msg = new ProducerRecord<byte[],byte[]>(topic, messageStr);
        try {
            this.producer.send(msg,callback);
        }catch( Exception e) {
            logger.error("cannot send msg " + msg, e);
            System.exit(1);
        }
    }
}
