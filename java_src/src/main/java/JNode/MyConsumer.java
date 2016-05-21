package JNode;


import com.ericsson.otp.erlang.OtpMbox;
import com.ericsson.otp.erlang.OtpNode;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MyConsumer {
    final static Logger logger = Logger.getLogger(MyConsumer.class);
    private final ConsumerConnector consumer;
    private MyConsumerConfig config;
    private ExecutorService executor;

    public MyConsumer(MyConsumerConfig config) {
        this.config = config;
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig());
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                logger.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void start() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(config.getTopic(), new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(config.getTopic());

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(config.getNumOfThreads());

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new MyConsumerThread(stream, threadNumber, config));
            threadNumber++;
            System.out.println("debugging " + threadNumber + " " + config.getMyself());
        }
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", config.getZookeeper());
        props.put("group.id", config.getGroupId());
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "10000");
        return new ConsumerConfig(props);
    }
}