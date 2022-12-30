package com.hzk.server;

import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;

public class InnerTaskManagerMain {

    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir", "D:\\tool\\hadoop");

        TaskManagerRunner.main(args);
    }

}
