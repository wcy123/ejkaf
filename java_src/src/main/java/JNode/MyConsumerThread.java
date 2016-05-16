package JNode;

/**
 * Created by wangchunye on 5/14/16.
 */
import com.ericsson.otp.erlang.*;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class MyConsumerThread implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private OtpMbox m_mbox;
    public MyConsumerThread(KafkaStream a_stream, OtpMbox mbox, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        m_mbox = mbox;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (true) {
            try {
                System.out.println(" waiting for message from erlang ");
                OtpErlangObject o = m_mbox.receive();
                System.out.println(" A new message from erlang " + o);
                if (o instanceof OtpErlangTuple) {
                    OtpErlangTuple msg = (OtpErlangTuple) o;
                    OtpErlangPid from = (OtpErlangPid)(msg.elementAt(0));
                    OtpErlangObject tag = msg.elementAt(1);
                    OtpErlangObject reply; //= new OtpErlangAtom("undefined");;
                    if (false || it.hasNext()) {
                        System.out.println("has next" + o);
                        String payload = new String(it.next().message());
                        reply =  new OtpErlangBinary(payload.getBytes());
                    }else {
                        reply =  new OtpErlangAtom("undefined");
                    }
                    m_mbox.send(from, gen_reply(tag,reply));
                }
            } catch (OtpErlangExit otpErlangExit) {
                otpErlangExit.printStackTrace();
                break;
            } catch (OtpErlangDecodeException e) {
                e.printStackTrace();
                break;
            }
        }
    }
    private OtpErlangObject gen_reply(OtpErlangObject tag, OtpErlangObject reply){
        OtpErlangObject[] result = new OtpErlangObject[2];
        result[0] = tag;
        result[1] = reply;
        return new OtpErlangTuple(result);
    }
}