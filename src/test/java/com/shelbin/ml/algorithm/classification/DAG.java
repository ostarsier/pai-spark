package com.shelbin.ml.algorithm.classification;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DAG {

    public static void main(String[] args) {
        Queue<Integer> taskReadyQueue = new ConcurrentLinkedQueue<>();
        Queue<Integer> taskCompleteQueue = new ConcurrentLinkedQueue<>();

        Thread masterThread = new MasterThread(taskReadyQueue, taskCompleteQueue);
        Thread workerThread = new WorkerThread(taskReadyQueue, taskCompleteQueue);

        masterThread.start();
        workerThread.start();

    }
}

class MasterThread extends Thread {
    private Queue<Integer> taskReadyQueue;
    private Queue<Integer> taskCompleteQueue;

    public MasterThread(Queue<Integer> taskReadyQueue, Queue<Integer> taskCompleteQueue) {
        this.taskReadyQueue = taskReadyQueue;
        this.taskCompleteQueue = taskCompleteQueue;
    }

    @Override
    public void run() {
        // Master逻辑
        // 在taskReadyQueue中添加任务
        for (int i = 1; i <= 10; i++) {
            taskReadyQueue.add(i);
            System.out.println("Master: Added task " + i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class WorkerThread extends Thread {
    private Queue<Integer> taskReadyQueue;
    private Queue<Integer> taskCompleteQueue;

    public WorkerThread(Queue<Integer> taskReadyQueue, Queue<Integer> taskCompleteQueue) {
        this.taskReadyQueue = taskReadyQueue;
        this.taskCompleteQueue = taskCompleteQueue;
    }

    @Override
    public void run() {
        // Worker逻辑
        while (true) {
            if (!taskReadyQueue.isEmpty()) {
                int taskId = taskReadyQueue.poll();
                System.out.println("Worker: Processing task " + taskId);
                // 执行任务
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 任务完成，将任务ID添加到taskCompleteQueue中
                taskCompleteQueue.add(taskId);
                System.out.println("Worker: Completed task " + taskId);
            }
        }
    }
}
