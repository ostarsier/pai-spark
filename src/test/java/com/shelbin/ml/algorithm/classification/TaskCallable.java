package com.shelbin.ml.algorithm.classification;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class TaskCallable implements Callable<Void> {
    private String taskId;

    public TaskCallable(String taskId) {
        this.taskId = taskId;
    }

    @Override
    public Void call() throws Exception {
        System.out.println("begin TaskId=" + taskId);
        TimeUnit.SECONDS.sleep(1);
        System.out.println("end TaskId=" + taskId);
        return null;
    }

}