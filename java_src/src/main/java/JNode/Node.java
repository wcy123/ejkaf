package JNode;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangPid;
import com.ericsson.otp.erlang.OtpErlangTuple;
import com.ericsson.otp.erlang.OtpMbox;
import com.ericsson.otp.erlang.OtpNode;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;


public class Node {
    final static Logger logger = Logger.getLogger(Node.class);
    MyProducer myProducer;
    public static void main (String[]args) {
        Node node = new Node();
        try {
            node.loop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    Node() {
        myProducer = new MyProducer();
    }

    void loop() throws IOException {
        String FirstNode = System.getenv("FIRST_NODE");
        if(FirstNode == null){
            logger.error("env FIRST_NODE is empty");
            System.exit(1);
        }
        OtpNode self;
        String Cookie = System.getenv("ERLANG_COOKIE");
        if(Cookie == null) {
            self = new OtpNode("java");
        } else {
            self = new OtpNode("java", Cookie);
        }
        OtpMbox msgBox = self.createMbox("kafka");
        if (!self.ping(FirstNode, 2000)) {
            logger.warn("unable to connect the first node " + FirstNode);
            //System.exit(1);
        }
        logger.info("java node is created.");
        OtpErlangObject c_exit = new OtpErlangAtom("exit");
        OtpErlangObject c_produce = new OtpErlangAtom("produce");
        ArrayList<MyConsumer> consumers = startConsumers(self);
        while (true) {
            try {
                OtpErlangObject o = msgBox.receive();
                if (o instanceof OtpErlangTuple) {
                    OtpErlangTuple msg = (OtpErlangTuple)o;
                    OtpErlangAtom name = (OtpErlangAtom)(msg.elementAt(0));
                    if(name.equals(c_produce)) {
                        HandleProduce(msgBox, msg);
                    }
                }else if( o.equals(c_exit) ) {
                    break;
                }
            } catch (OtpErlangExit otpErlangExit) {
                otpErlangExit.printStackTrace();
            } catch (OtpErlangDecodeException e) {
                e.printStackTrace();
            }
        }
        for(MyConsumer consumer : consumers) {
            consumer.shutdown();
        }
    }
    ArrayList<MyConsumer> startConsumers(OtpNode self) {
        String[] configs = {"migrate"};
        Properties prop = new Properties();
        String propFileName = "topics.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        if (inputStream != null) {
            try {
                prop.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("cannot read topic");
                System.exit(1);
            }
        } else {
            logger.error("property file '" + propFileName + "' not found");
            System.exit(1);
        }

        ArrayList<MyConsumer> ret = new ArrayList<MyConsumer>();
        for (String c : configs) {
            String zk = prop.getProperty("kafka." + c + ".zookeeper");
            String groupid = prop.getProperty("kafka." + c + ".group_id");
            String topic = prop.getProperty("kafka." + c + ".topic");
            int numOfThreads = Integer.parseInt(prop.getProperty("kafka." + c + ".num_of_threads"));
            MyConsumer consumer = startConsumerThreadPool(self, zk, groupid, topic, numOfThreads);
            ret.add(consumer);
        }
        return ret;
    }
    MyConsumer startConsumerThreadPool(OtpNode self, String zk,
                                        String groupid, String topic,
                                       int numOfThreads)
    {
        OtpMbox mbox = self.createMbox(topic);
        MyConsumer ret = new MyConsumer(zk, groupid, topic);
        ret.start(mbox, numOfThreads);
        return ret;
    }
    void HandleProduce(OtpMbox msgBox, OtpErlangTuple msg)
    {
        OtpErlangPid from = (OtpErlangPid)(msg.elementAt(1));
        OtpErlangObject ref = msg.elementAt(2);
        OtpErlangBinary topic = (OtpErlangBinary)msg.elementAt(3);
        OtpErlangBinary data = (OtpErlangBinary)msg.elementAt(4);
        this.myProducer.send(new String(topic.binaryValue()),
                data.binaryValue(),
                new KafkaCallback(msgBox, from, ref));
    }

}
class KafkaCallback implements Callback {

    private final OtpErlangObject tag;
    private final OtpMbox msgBox;
    private final OtpErlangPid from;
    public KafkaCallback(OtpMbox msgBox, OtpErlangPid from, OtpErlangObject tag) {
        this.msgBox = msgBox;
        this.from = from;
        this.tag = tag;
    }
     public void onCompletion(RecordMetadata metadata, Exception exception) {
         OtpErlangObject reply;
         if (metadata != null) {
             reply = new OtpErlangAtom("ok");
         } else {
             // return {error, Reason}.
             OtpErlangObject[] error = new OtpErlangObject[2];
             error[0] = new OtpErlangAtom("error");
             String reason = exception.toString();
             error[1] = new OtpErlangBinary(reason);
             reply = new OtpErlangTuple(error);
        }
        msgBox.send(from, gen_reply(tag,reply));
    }
    private OtpErlangObject gen_reply(OtpErlangObject tag, OtpErlangObject reply){
        OtpErlangObject[] result = new OtpErlangObject[2];
        result[0] = tag;
        result[1] = reply;
        return new OtpErlangTuple(result);
    }
}
