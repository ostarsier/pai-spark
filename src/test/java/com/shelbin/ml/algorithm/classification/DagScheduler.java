package com.shelbin.ml.algorithm.classification;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DagScheduler {


    private List<Edge> edges = new ArrayList<>();
    private String stopTaskId;

    /**
     * 已经运行完成的任务
     */
    private Map<String, ListenableFuture<Void>> taskFutures = new HashMap<>();

    private Map<String, List<String>> childrenMap;

    private Map<String, List<String>> dependencyMap;

    private ExecutorService executorService = Executors.newFixedThreadPool(10);
    private ListeningExecutorService service = MoreExecutors.listeningDecorator(executorService);

    public void init() {
        this.childrenMap = edges.stream().collect(Collectors.groupingBy(Edge::getFrom, Collectors.mapping(Edge::getTo, Collectors.toList())));
        this.dependencyMap = edges.stream().collect(Collectors.groupingBy(Edge::getTo, Collectors.mapping(Edge::getFrom, Collectors.toList())));
    }


    /**
     * 1、上游节点未完成（taskFutures里没数据），退出
     * 2、提交任务
     * 3、任务完成之后：提交children
     */
    private class TaskSubmitRunnable implements Runnable {

        private String taskId;

        public TaskSubmitRunnable(String taskId) {
            this.taskId = taskId;
        }

        @Override
        public void run() {
            submitTask(taskId);
        }
        private void submitTask(String taskId) {
            System.out.println("submitTask " + taskId);
//       1、上游节点未完成，退出
            List<ListenableFuture<Void>> dependencyTaskFutures = new ArrayList<>();
            for (String dependencyTaskId : dependencyMap.get(taskId)) {
                ListenableFuture<Void> dependencyTaskFuture = taskFutures.get(dependencyTaskId);
                if (dependencyTaskFuture == null) {
                    System.out.println("taskId=" + taskId + "依赖的" + dependencyTaskId + "没有开始运行，退出");
                    return;
                }
                dependencyTaskFutures.add(dependencyTaskFuture);
            }
//            2、提交任务
            ListenableFuture<Void> taskFuture = Futures.whenAllSucceed(dependencyTaskFutures).call(new TaskCallable(taskId), service);
//            3、任务完成之后：提交children
            taskFuture.addListener(new SubmitChildrenRunnable(taskId, taskFuture), executorService);
        }
    }


    @AllArgsConstructor
    @Data
    private static class Edge {
        String from;
        String to;
    }

    public void addEdge(String fromTaskId, String toTaskId) {
        edges.add(new Edge(fromTaskId, toTaskId));
    }

    private class SubmitChildrenRunnable implements Runnable {

        private String taskId;
        private ListenableFuture<Void> taskFuture;

        public SubmitChildrenRunnable(String startId, ListenableFuture<Void> taskFuture) {
            this.taskId = startId;
            this.taskFuture = taskFuture;
        }

        @Override
        public void run() {
            taskFutures.put(taskId, taskFuture);
            if(stopTaskId.equals(taskId)){
                System.out.println("任务到此节点结束，不再提交children " + taskId);
                return;
            }
            List<String> childTaskIdList = childrenMap.get(taskId);
            if (childTaskIdList == null) {
                return;
            }
            for (String childTaskId : childTaskIdList) {
                executorService.submit(new TaskSubmitRunnable(childTaskId));
            }
        }
    }

    private void start(String startTaskId, String stopTaskId) {
        this.stopTaskId = stopTaskId;
        ListenableFuture<Void> startTask = service.submit(new TaskCallable(startTaskId));
        startTask.addListener(new SubmitChildrenRunnable(startTaskId, startTask), executorService);


    }

    public static void main(String[] args) {
        DagScheduler dagScheduler = new DagScheduler();
        dagScheduler.addEdge("start", "dataSourceTask");
        dagScheduler.addEdge("dataSourceTask", "splitTask");
        dagScheduler.addEdge("splitTask", "regressionModelTask");
        dagScheduler.addEdge("splitTask", "predictionTask");
        dagScheduler.addEdge("regressionModelTask", "predictionTask");
        dagScheduler.init();

        dagScheduler.start("start", "");
    }


}
