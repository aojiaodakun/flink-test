package com.hzk.server;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class JobManagerMain {

    private static final Logger LOG = LoggerFactory.getLogger(JobManagerMain.class);

    public static void main(String[] args) throws Exception{
        EnvironmentInformation.logEnvironmentInfo(LOG, JobManagerMain.class.getSimpleName(), new String[]{});
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);


        Configuration configuration = loadConfiguration();
        //启动StandaloneSessionClusterEntrypoint
        StandaloneSessionClusterEntrypoint entrypoint = new StandaloneSessionClusterEntrypoint(configuration);
        ClusterEntrypoint.runClusterEntrypoint(entrypoint);

    }


    public static Configuration loadConfiguration(){
        Configuration configuration = new Configuration();
        int masterWebPort = 7760;
        configuration.setInteger(RestOptions.PORT, masterWebPort);

        String clusterId = "local_hzk";
        String storageDir = System.getProperty("java.io.tmpdir");
        String zookeeper = "localhost:2181";
        configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zookeeper);
        configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, storageDir);

        configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT, "flink");
        int masterHeap = 1024;
        configuration.setInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB, masterHeap);
        configuration.set(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, MemorySize.ofMebiBytes(masterHeap));

        Integer jobStore_cacheCount = 2000; //2000
        Long jobStore_cacheSize = 200L * 1024 * 1024; //200M
        Long jobStore_expireTime = 24 * 60L * 60L; //24 hours

        configuration.setInteger(JobManagerOptions.JOB_STORE_MAX_CAPACITY, jobStore_cacheCount);
        configuration.setLong(JobManagerOptions.JOB_STORE_CACHE_SIZE, jobStore_cacheSize);
        configuration.setLong(JobManagerOptions.JOB_STORE_EXPIRATION_TIME, jobStore_expireTime);

        configuration.setBoolean(ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY, true);

        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
            configuration.setString(JobManagerOptions.ADDRESS, host);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        configuration.setBoolean(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME, false);

        String webUrl = "http://" + host + ":" + masterWebPort;

        configuration.setLong(WebOptions.REFRESH_INTERVAL, 10000L); //10 seconds

        /**
         * common
         */
        configuration.set(HighAvailabilityOptions.ZOOKEEPER_RETRY_WAIT, 5000);
        configuration.set(HighAvailabilityOptions.ZOOKEEPER_MAX_RETRY_ATTEMPTS, 120);
        configuration.set(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, 120000);

        Integer value = 180;
        configuration.setString(AkkaOptions.ASK_TIMEOUT, value +" s");
        value = 60;
        configuration.setString(AkkaOptions.TCP_TIMEOUT, value + " s");
        configuration.setString(AkkaOptions.LOOKUP_TIMEOUT, "60 s");

        value = 10000;
        configuration.setLong(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, value);
        value = 300000;
        configuration.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, value);

        value = 180000;
        configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, value);

        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
        configuration.setBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, false);

        String dirs = System.getProperty("java.io.tmpdir") + "/algo"; //algox.io.tmp.dirs
        configuration.setString(CoreOptions.TMP_DIRS, dirs);

        /**
         * akka
         */
        configuration.setString("akka.artery", "old");
        configuration.setString(AkkaOptions.FRAMESIZE, "100M");


        return configuration;
    }

}
