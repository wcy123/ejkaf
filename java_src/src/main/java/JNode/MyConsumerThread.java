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
    private KafkaStream stream;
    private MyConsumerConfig config;
    public MyConsumerThread(KafkaStream stream,
                            long nThreadIndex,
                            MyConsumerConfig config
                            ) {
        this.config = config;
        this.stream = stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        OtpErlangObject c_undefined = new OtpErlangAtom("undefined");
        OtpMbox mbox = config.getMyself().createMbox();
        OtpErlangObject moduleName = new OtpErlangAtom(config.getModuleName());
        while (true) {
            OtpErlangObject msg = c_undefined;
            logger.debug("start to read topic");
            if (it.hasNext()) {
                String payload = new String(it.next().message());
                msg =  new OtpErlangBinary(payload.getBytes());
            }else {
                logger.error("exist, no kafka connection");
                break;
            }
            OtpErlangObject ref = send_request(mbox, msg);
            if(!wait_for_response(mbox,ref)) {
                logger.error("no response, exit\n");
                break;
            }

        }
    }
    OtpErlangObject send_request(OtpMbox mbox, OtpErlangObject msg){
        OtpErlangObject ref = config.getMyself().createRef();
        OtpErlangObject[] request0 = new OtpErlangObject[4];
        request0[0] = new OtpErlangBinary(config.getTopic());
        request0[1] = mbox.self();
        request0[2] = ref;
        request0[3] = msg;
        OtpErlangObject request = new OtpErlangTuple(request0);
        String NodeName = System.getenv("FIRST_NODE");
        logger.debug("sending " + msg + " to {"  + config.getModuleName() + "," + NodeName + "@localhost }");
        mbox.send(config.getModuleName(), NodeName, request);
        return ref;
    }
    boolean wait_for_response(OtpMbox mbox, OtpErlangObject ref) {
        long timeout = 1000;
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
