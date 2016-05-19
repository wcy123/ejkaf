package JNode;

/**
 * Created by wangchunye on 5/14/16.
 */
import com.ericsson.otp.erlang.*;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.log4j.Logger;


public class MyConsumerThread implements Runnable {
    final static Logger logger = Logger.getLogger(MyConsumerThread.class);
    private KafkaStream m_stream;
    private int m_threadNumber;
    private OtpNode m_self;
    private String m_node;
    private String m_remote_name;
    public MyConsumerThread(KafkaStream a_stream, OtpNode self, int a_threadNumber, String RemoteName, String Node) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        m_self = self;
        m_node = Node;
        m_remote_name = RemoteName;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        OtpErlangObject c_undefined = new OtpErlangAtom("undefined");
        OtpMbox mbox = m_self.createMbox();
        while (true) {
            OtpErlangObject msg = c_undefined;
            logger.debug("start to read topic");
            if (it.hasNext()) {
                String payload = new String(it.next().message());
                msg =  new OtpErlangBinary(payload.getBytes());
            }else {
                System.out.println("exist, no kafka connection");
                break;
            }
            OtpErlangObject ref = send_request(mbox,msg);
            if(!wait_for_response(mbox,ref)) {
                logger.info("no response, exit\n");
                break;
            }

        }
    }
    OtpErlangObject send_request(OtpMbox mbox, OtpErlangObject msg){
        OtpErlangObject ref = m_self.createRef();
        OtpErlangObject[] request0 = new OtpErlangObject[3];
        request0[0] = mbox.self();
        request0[1] = ref;
        request0[2] = msg;
        OtpErlangObject request = new OtpErlangTuple(request0);
        logger.debug("sending " + msg + " to "  + m_remote_name + "@" + m_node);
        mbox.send(m_remote_name, m_node, request);
        return ref;
    }
    boolean wait_for_response(OtpMbox mbox, OtpErlangObject ref) {
        long timeout = 10000;
        boolean ack = false;
        OtpErlangObject o;
        while (!ack) {
            try {
                o = mbox.receive(timeout);
            } catch (OtpErlangExit otpErlangExit) {
                otpErlangExit.printStackTrace();
                break;
            } catch (OtpErlangDecodeException e) {
                e.printStackTrace();
                break;
            }
            if (o != null){
                if(o.equals(ref)) {
                    ack = true;
                }
            } else {
                // break if timeout
                break;
            }
        }
        return ack;
    }
}