package JNode;

import com.ericsson.otp.erlang.*;
import java.io.IOException;


/**
 * Created by wangchunye on 3/13/16.
 */
public class Node {
    Producer producer;
    public static void main (String[]args) {
        new Thread(new Runnable() {
            public void run() {
                try {
                    System.out.println("start to read from standard input");
                    int buf = System.in.read();
                    System.out.println("receive:" + buf);
                } catch (IOException e) {
                    System.exit(0);
                }
            }}).start();
        Node node = new Node();
        try {
            node.loop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    Node() {
        producer = new Producer();
    }
    void loop() throws IOException {
        String FirstNode = System.getenv("FIRST_NODE");
        if(FirstNode == null){
            System.err.println("env FIRST_NODE is empty");
            System.exit(1);
        }
        OtpNode self = new OtpNode("java");
        OtpMbox msgBox = self.createMbox("kafka");
        if (!self.ping(FirstNode, 2000)) {
            System.err.println("unable to connect the first node " + FirstNode);
            System.exit(1);
        }
        System.out.println("java node is created.");
        OtpErlangObject exit = new OtpErlangAtom("exit");
        while (true) {
            try {
                OtpErlangObject o = msgBox.receive();
                System.out.println("Hello " + o + "   " + o.equals(exit));
                if (o instanceof OtpErlangTuple) {
                    OtpErlangTuple msg = (OtpErlangTuple)o;

                    OtpErlangPid from = (OtpErlangPid)(msg.elementAt(0));
                    OtpErlangObject ref = msg.elementAt(1);
                    OtpErlangBinary topic = (OtpErlangBinary)msg.elementAt(2);
                    OtpErlangBinary data = (OtpErlangBinary)msg.elementAt(3);
                    this.producer.send(new String(topic.binaryValue()), new String(data.binaryValue()));

                    OtpErlangObject[] result = new OtpErlangObject[2];
                    result[0] = ref;
                    result[1] = new OtpErlangAtom("ok");
                    msgBox.send(from, new OtpErlangTuple(result));
                }else if( o.equals(exit) ) {
                    System.exit(0);
                }
            } catch (OtpErlangExit otpErlangExit) {
                otpErlangExit.printStackTrace();
            } catch (OtpErlangDecodeException e) {
                e.printStackTrace();
            }
        }
    }
}
