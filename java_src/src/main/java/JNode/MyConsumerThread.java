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
    private long counter = 0;
    private long nThreadIndex = 0;
    public MyConsumerThread(KafkaStream stream,
                            long nThreadIndex,
                            MyConsumerConfig config
                            ) {
        this.config = config;
        this.stream = stream;
        this.nThreadIndex = nThreadIndex;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        OtpErlangObject c_undefined = new OtpErlangAtom("undefined");
        OtpMbox mbox = config.getMyself().createMbox();
        while (it.hasNext()) {
            counter ++;
            if (counter % 10000 == 0) {
                logger.info("thread " + nThreadIndex + " consumes " + counter +  " msg");
            }
            String payload = new String(it.next().message());
            if (config.isValid(payload)) {
                String NodeName = System.getenv("FIRST_NODE");
                logger.info("sending " + payload + " to {"  + config.getModuleName() + "," + NodeName + " }");
                OtpErlangObject msg = new OtpErlangBinary(payload.getBytes());
                OtpErlangObject ref = send_request(mbox, msg, NodeName);
                if (!wait_for_response(mbox, ref)) {
                    logger.error("no response, exit");
                    break;
                }
            }
        }
    }
    private OtpErlangObject send_request(OtpMbox mbox, OtpErlangObject msg, String NodeName){
        OtpErlangObject ref = config.getMyself().createRef();
        OtpErlangObject[] request0 = new OtpErlangObject[4];
        request0[0] = new OtpErlangBinary(config.getTopic());
        request0[1] = mbox.self();
        request0[2] = ref;
        request0[3] = msg;
        OtpErlangObject request = new OtpErlangTuple(request0);
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
