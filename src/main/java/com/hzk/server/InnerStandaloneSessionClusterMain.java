package com.hzk.server;

import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;

/**
 * 包含JobManager，ResourceManager，Dispatcher
 */
public class InnerStandaloneSessionClusterMain {

    public static void main(String[] args) throws Exception{

        System.setProperty("hadoop.home.dir", "D:\\tool\\hadoop");
        StandaloneSessionClusterEntrypoint.main(args);
    }

}
