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
    private long timeout = 60000;
    private OtpNode myself = null;
    private String [] filter = null;
    public MyConsumerConfig(Properties prop, String module, OtpNode myself) {
        this.topic = prop.getProperty("kafka." + module + ".topic", topic);
        moduleName = prop.getProperty("kafka." + module + ".module_name", module);
        zookeeper = prop.getProperty("kafka." + module + ".zookeeper", zookeeper);
        groupId = prop.getProperty("kafka." + module + ".group_id", groupId);
        numOfThreads = Integer.parseInt(prop.getProperty("kafka." + module + ".num_of_threads", "1"));
        String filterString = prop.getProperty("kafka." + module + ".filter", "");
        filter = filterString.split(",");
        this.myself = myself;
        timeout = Integer.parseInt(prop.getProperty("timeout"));
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

    public OtpNode getMyself() {
        return myself;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isValid(String s) {
        boolean ret = false;
        if(filter.length == 0) return true;
        for(String ff: filter){
            ret = ret || s.contains(ff);
        }
        return ret;
    }
}


