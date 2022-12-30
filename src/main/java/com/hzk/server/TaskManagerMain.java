package com.hzk.server;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskManagerMain {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerMain.class);

    public static void main(String[] args) throws Exception{
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, TaskManagerMain.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);


        // 启动TaskManagerRunner
        Configuration configuration = JobManagerMain.loadConfiguration();

        configuration.setString("taskmanager.cpu.cores", "1.0");
        configuration.setString("taskmanager.memory.task.heap.size", "402653174b");
        configuration.setString("taskmanager.memory.managed.size", "536870920b");
        configuration.setString("taskmanager.memory.network.min", "134217730b");
        configuration.setString("taskmanager.memory.network.max", "134217730b");
        configuration.setString("taskmanager.memory.framework.heap.size", "134217728b");
        configuration.setString("taskmanager.memory.managed.size", "536870920b");
        TaskManagerRunner.runTaskManagerProcessSecurely(configuration);

        System.in.read();
    }


}
