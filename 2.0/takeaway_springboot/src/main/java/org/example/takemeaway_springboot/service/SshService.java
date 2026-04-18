package org.example.takeaway_springboot.service;

import com.jcraft.jsch.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SshService {

    @Value("${ssh.host}")
    private String host;

    @Value("${ssh.port}")
    private int port;

    @Value("${ssh.user}")
    private String user;

    @Value("${ssh.password}")
    private String password;

    // 存储任务状态（保持不变）
    private final ConcurrentHashMap<String, SparkTaskInfo> taskStatusMap = new ConcurrentHashMap<>();

    public static class SparkTaskInfo {
        public String status;   // RUNNING, SUCCESS, FAILED
        public String log;
        public String error;
        public long startTime;
        public Long endTime;
    }

    /**
     * 获取所有任务状态（保持不变）
     */
    public ConcurrentHashMap<String, SparkTaskInfo> getAllTaskStatus() {
        return taskStatusMap;
    }

    /**
     * 异步执行Spark分析（新增）
     */
    public String executeSparkSubmitAsync(String fileName) {
        String taskId = fileName + "_" + System.currentTimeMillis();

        SparkTaskInfo taskInfo = new SparkTaskInfo();
        taskInfo.status = "RUNNING";
        taskInfo.log = "任务已提交，等待执行...\n";
        taskInfo.startTime = System.currentTimeMillis();
        taskStatusMap.put(taskId, taskInfo);

        // 异步执行
        CompletableFuture.runAsync(() -> {
            executeSparkSubmitSync(taskId, fileName);
        });

        return taskId;
    }

    /**
     * 同步执行Spark分析（实际执行方法）
     */
    private void executeSparkSubmitSync(String taskId, String fileName) {
        SparkTaskInfo taskInfo = taskStatusMap.get(taskId);
        Session session = null;
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(user, host, port);
            session.setPassword(password);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            String jarPath = "/home/hadoop/takeaway_spark.jar";
            String mainClass = "analysis.RunAllAnalysis";

            String command = String.format(
                    "spark-submit --class %s --master local[*] %s 2>&1",
                    mainClass, jarPath
            );

            taskInfo.log += ">>> 命令: " + command + "\n";
            taskInfo.log += ">>> 开始执行...\n";

            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);

            InputStream in = channel.getInputStream();
            InputStream err = channel.getErrStream();
            channel.connect();

            StringBuilder output = new StringBuilder();
            byte[] buffer = new byte[1024];
            int bytesRead;

            long lastHeartbeat = System.currentTimeMillis();
            while (true) {
                while (in.available() > 0) {
                    bytesRead = in.read(buffer);
                    if (bytesRead != -1) {
                        String chunk = new String(buffer, 0, bytesRead);
                        output.append(chunk);
                        taskInfo.log += chunk;
                        lastHeartbeat = System.currentTimeMillis();
                    }
                }
                while (err.available() > 0) {
                    bytesRead = err.read(buffer);
                    if (bytesRead != -1) {
                        taskInfo.log += new String(buffer, 0, bytesRead);
                    }
                }
                if (channel.isClosed()) {
                    break;
                }
                // 每30秒输出心跳
                if (System.currentTimeMillis() - lastHeartbeat > 30000) {
                    taskInfo.log += ">>> 任务执行中，请稍候...\n";
                    lastHeartbeat = System.currentTimeMillis();
                }
                Thread.sleep(500);
            }

            int exitCode = channel.getExitStatus();
            channel.disconnect();

            taskInfo.endTime = System.currentTimeMillis();

            if (exitCode == 0) {
                taskInfo.status = "SUCCESS";
                taskInfo.log += "\n>>> 任务执行成功！耗时: " +
                        (taskInfo.endTime - taskInfo.startTime) / 1000 + "秒\n";
            } else {
                taskInfo.status = "FAILED";
                taskInfo.log += "\n>>> 任务执行失败，退出码: " + exitCode + "\n";
            }

        } catch (Exception e) {
            taskInfo.status = "FAILED";
            taskInfo.log += "\n>>> 异常: " + e.getMessage() + "\n";
            e.printStackTrace();
        } finally {
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        }
    }
}