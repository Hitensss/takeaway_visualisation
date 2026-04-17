package org.example.takeaway_springboot.service;

import com.jcraft.jsch.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Properties;
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

    // 存储任务状态
    private final ConcurrentHashMap<String, SparkTaskInfo> taskStatusMap = new ConcurrentHashMap<>();

    public static class SparkTaskInfo {
        public String status;   // RUNNING, SUCCESS, FAILED
        public String log;
        public String error;
        public long startTime;
        public Long endTime;
    }

    /**
     * 获取所有任务状态
     */
    public ConcurrentHashMap<String, SparkTaskInfo> getAllTaskStatus() {
        return taskStatusMap;
    }

    /**
     * 在虚拟机上执行 spark-submit 命令
     */
    public boolean executeSparkSubmit(String fileName) {
        String taskId = fileName + "_" + System.currentTimeMillis();
        SparkTaskInfo taskInfo = new SparkTaskInfo();
        taskInfo.status = "RUNNING";
        taskInfo.log = "";
        taskInfo.startTime = System.currentTimeMillis();
        taskStatusMap.put(taskId, taskInfo);

        Session session = null;
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(user, host, port);
            session.setPassword(password);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            // JAR包路径和主类（注意：不传递文件路径参数）
            String jarPath = "/home/hadoop/takeaway_spark.jar";
            String mainClass = "analysis.RunAllAnalysis";

            // spark-submit 命令
            String command = String.format(
                    "spark-submit --class %s --master local[*] %s 2>&1",
                    mainClass, jarPath
            );

            System.out.println("执行命令: " + command);
            taskInfo.log += ">>> 命令: " + command + "\n";

            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);

            InputStream in = channel.getInputStream();
            InputStream err = channel.getErrStream();
            channel.connect();

            StringBuilder output = new StringBuilder();
            StringBuilder errorOutput = new StringBuilder();

            byte[] buffer = new byte[1024];
            int bytesRead;

            while (true) {
                while (in.available() > 0) {
                    bytesRead = in.read(buffer);
                    if (bytesRead != -1) {
                        String chunk = new String(buffer, 0, bytesRead);
                        output.append(chunk);
                        taskInfo.log += chunk;
                        System.out.print(chunk);
                    }
                }
                while (err.available() > 0) {
                    bytesRead = err.read(buffer);
                    if (bytesRead != -1) {
                        errorOutput.append(new String(buffer, 0, bytesRead));
                    }
                }
                if (channel.isClosed()) {
                    break;
                }
                try { Thread.sleep(100); } catch (Exception e) {}
            }

            int exitCode = channel.getExitStatus();
            channel.disconnect();

            taskInfo.endTime = System.currentTimeMillis();

            if (exitCode == 0) {
                taskInfo.status = "SUCCESS";
                taskInfo.log += "\n>>> 任务执行成功！\n";
                System.out.println("Spark任务执行成功");
                return true;
            } else {
                taskInfo.status = "FAILED";
                taskInfo.error = errorOutput.toString();
                taskInfo.log += "\n>>> 任务执行失败，退出码: " + exitCode + "\n";
                taskInfo.log += ">>> 错误信息: " + errorOutput.toString() + "\n";
                System.err.println("Spark任务执行失败");
                return false;
            }
        } catch (Exception e) {
            taskInfo.status = "FAILED";
            taskInfo.error = e.getMessage();
            taskInfo.log += "\n>>> 异常: " + e.getMessage() + "\n";
            e.printStackTrace();
            return false;
        } finally {
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        }
    }
}