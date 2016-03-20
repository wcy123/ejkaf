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

import java.io.IOException;


public class Node {
    MyProducer myProducer;
    public static void main (String[]args) {
        new Thread(new Runnable() {
            public void run() {
                try {
                    //System.out.println("start to read from standard input");
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
        myProducer = new MyProducer();
    }
    void loop() throws IOException {
        String FirstNode = System.getenv("FIRST_NODE");
        if(FirstNode == null){
            System.err.println("env FIRST_NODE is empty");
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
            System.err.println("unable to connect the first node " + FirstNode);
            System.exit(1);
        }
        System.out.println("java node is created.");
        OtpErlangObject exit = new OtpErlangAtom("exit");
        while (true) {
            try {
                OtpErlangObject o = msgBox.receive();
                if (o instanceof OtpErlangTuple) {
                    OtpErlangTuple msg = (OtpErlangTuple)o;

                    OtpErlangPid from = (OtpErlangPid)(msg.elementAt(0));
                    OtpErlangObject ref = msg.elementAt(1);
                    OtpErlangBinary topic = (OtpErlangBinary)msg.elementAt(2);
                    OtpErlangBinary data = (OtpErlangBinary)msg.elementAt(3);
                    this.myProducer.send(new String(topic.binaryValue()), data.binaryValue());

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
