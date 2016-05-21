package JNode;

import com.ericsson.otp.erlang.OtpNode;

import java.util.Properties;

/**
 * Created by wangchunye on 5/21/16.
 */
public class MyConsumerConfig {
    private int numOfThreads = 1;
    private String topic = "topic1";
    private String zookeeper = "zk:2181";
    private String groupId = "my_group_id";
    private String moduleName = "module";
    private String nodeName = "im_libs";
    private OtpNode myself = null;

    public MyConsumerConfig(Properties prop, String module, OtpNode myself) {
        this.topic = prop.getProperty("kafka." + module + ".topic", topic);
        moduleName = prop.getProperty("kafka." + module + ".module_name", module);
        nodeName = prop.getProperty("kafka." + module + ".node_name", nodeName);
        zookeeper = prop.getProperty("kafka." + module + ".zookeeper",zookeeper);
        groupId = prop.getProperty("kafka." + module + ".group_id", groupId);
        numOfThreads = Integer.parseInt(prop.getProperty("kafka." + module + ".num_of_threads", "1"));
        this.myself = myself;
    }

    public int getNumOfThreads() {
        return numOfThreads;
    }

    public String getTopic() {
        return topic;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getModuleName() {
        return moduleName;
    }

    public String getNodeName() {
        return nodeName;
    }

    public OtpNode getMyself() {
        return myself;
    }
}


