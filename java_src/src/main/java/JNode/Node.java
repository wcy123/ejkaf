package JNode;

import com.ericsson.otp.erlang.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Properties;


public class Node {
    final static Logger logger = Logger.getLogger(Node.class);
    final private Properties prop;
    MyProducer myProducer;
    public static void main (String[]args) {
        Socket x = null;
     /*   try {
            x = new Socket("wangchunye", 4369);
            logger.info("socket is " + x);
            x.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);*/
        Properties p = System.getProperties();
        p.setProperty("OtpConnection.trace","4");
        System.setProperties(p);
        Node node = new Node();
        try {
            node.loop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    Node() {
        prop = InitProp();
        if(prop.getProperty("start_producer") == "true") {
            myProducer = new MyProducer();
        }
    }
    private Properties InitProp() {
        Properties prop1 = new Properties();
        String propFileName = "topics.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        if (inputStream != null) {
            try {
                prop1.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("cannot read topic");
                System.exit(1);
            }
        } else {
            logger.error("property file '" + propFileName + "' not found");
            System.exit(1);
        }
        return prop1;
    }
    void loop() throws IOException {
        String FirstNode = System.getenv("FIRST_NODE");
        if (FirstNode == null) {
            logger.error("env FIRST_NODE is empty");
            System.exit(1);
        }
        OtpNode self;
        String Cookie = System.getenv("ERLANG_COOKIE");
        if (Cookie == null) {
            self = new OtpNode("java");
        } else {
            self = new OtpNode("java", Cookie);
        }
        OtpMbox msgBox = self.createMbox("kafka");
        if (false && !self.ping(FirstNode, 2000)) {
            logger.warn("unable to connect the first node " + FirstNode);
            //System.exit(1);
        }
        logger.info("java node is created.");
        ArrayList<MyConsumer> consumers = startConsumers(self);
        OtpErlangObject c_exit = new OtpErlangAtom("exit");
        OtpErlangObject c_produce = new OtpErlangAtom("produce");
        while (true) {
            try {
                OtpErlangObject o = msgBox.receive();
                if (o instanceof OtpErlangTuple) {
                    OtpErlangTuple msg = (OtpErlangTuple) o;
                    OtpErlangAtom name = (OtpErlangAtom) (msg.elementAt(0));
                    if (name.equals(c_produce)) {
                        HandleProduce(msgBox, msg);
                    }
                } else if (o.equals(c_exit)) {
                    break;
                }
            } catch (OtpErlangExit otpErlangExit) {
                otpErlangExit.printStackTrace();
            } catch (OtpErlangDecodeException e) {
                e.printStackTrace();
            }
        }
        logger.info("shutting down the java kafka\n");
        for (MyConsumer consumer : consumers) {
            consumer.shutdown();
        }
    }
    ArrayList<MyConsumer> startConsumers(OtpNode self) {


        String[] modules  = prop.getProperty("modules").split(",", 0);

        ArrayList<MyConsumer> ret = new ArrayList<MyConsumer>();
        for (String module : modules) {
            MyConsumerConfig config = new MyConsumerConfig(prop, module, self);
            String Node = config.getNodeName();
            boolean node_ok = connect_node(self, Node);
            if(!node_ok) {
                System.exit(2);
            }
            MyConsumer consumer = startConsumerThreadPool(config);
            ret.add(consumer);
        }
        return ret;
    }
    boolean connect_node(OtpNode self, String node){
        try {
            logger.info("names = " + String.join("     \n", OtpEpmd.lookupNames()));
            OtpPeer peer = new OtpPeer("im_libs@wangchunye");
            logger.info("lookup port " + OtpEpmd.lookupPort(peer));
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean node_ok = false;
        for(int i = 0; i < 30; i ++){
            if (!self.ping(node, 2000)) {
                logger.error("unable to connect the sink node " + node);
            }else{
                node_ok = true;break;
            }
        }
        return node_ok;
    }
    MyConsumer startConsumerThreadPool(MyConsumerConfig config)
    {
        MyConsumer ret = new MyConsumer(config);
        ret.start();
        return ret;
    }
    void HandleProduce(OtpMbox msgBox, OtpErlangTuple msg)
    {
        OtpErlangPid from = (OtpErlangPid)(msg.elementAt(1));
        OtpErlangObject ref = msg.elementAt(2);
        OtpErlangBinary topic = (OtpErlangBinary)msg.elementAt(3);
        OtpErlangBinary data = (OtpErlangBinary)msg.elementAt(4);
        if (myProducer != null) {
            this.myProducer.send(new String(topic.binaryValue()),
                    data.binaryValue(),
                    new KafkaCallback(msgBox, from, ref));
        } else {
            OtpErlangObject reply;
            // return {error, Reason}.
            OtpErlangObject[] error = new OtpErlangObject[2];
            error[0] = new OtpErlangAtom("error");
            String reason = "not started";
            error[1] = new OtpErlangBinary(reason);
            reply = new OtpErlangTuple(error);

            OtpErlangObject[] result = new OtpErlangObject[2];
            result[0] = ref;
            result[1] = reply;
            msgBox.send(from, new OtpErlangTuple(result));
        }
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
