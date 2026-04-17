package org.example.takeaway_springboot.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;



@Service
public class WebHdfsService {

    @Value("${webhdfs.url}")
    private String webHdfsUrl;

    @Value("${hdfs.user}")
    private String hdfsUser;

    // 在类中添加 ObjectMapper
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 列出 HDFS 目录下的文件
     */
    public List<Map<String, Object>> listFiles() throws Exception {
        String listUrl = webHdfsUrl + "/food_data/?op=LISTSTATUS&user.name=" + hdfsUser;
        System.out.println("列出文件: " + listUrl);

        HttpURLConnection conn = (HttpURLConnection) new URL(listUrl).openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(30000);

        int responseCode = conn.getResponseCode();
        if (responseCode == 200) {
            try (InputStream is = conn.getInputStream();
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                return parseFileListFromJson(sb.toString());
            }
        }
        return new ArrayList<>();
    }

    /**
     * 解析 WebHDFS 返回的 JSON，提取文件列表
     */
    private List<Map<String, Object>> parseFileListFromJson(String json) throws Exception {
        List<Map<String, Object>> fileList = new ArrayList<>();

        JsonNode root = objectMapper.readTree(json);
        JsonNode fileStatuses = root.path("FileStatuses").path("FileStatus");

        if (fileStatuses.isArray()) {
            for (JsonNode node : fileStatuses) {
                Map<String, Object> fileInfo = new HashMap<>();
                fileInfo.put("name", node.path("pathSuffix").asText());
                fileInfo.put("path", "/food_data/" + node.path("pathSuffix").asText());
                fileInfo.put("size", node.path("length").asLong());
                fileInfo.put("modificationTime", node.path("modificationTime").asLong());
                fileList.add(fileInfo);
            }
        }

        System.out.println("解析到 " + fileList.size() + " 个文件");
        return fileList;
    }

    /**
     * 删除 HDFS 文件
     */
    public boolean deleteFile(String fileName) throws Exception {
        String deleteUrl = webHdfsUrl + "/food_data/" + fileName + "?op=DELETE&user.name=" + hdfsUser + "&recursive=false";
        System.out.println("删除文件: " + deleteUrl);

        HttpURLConnection conn = (HttpURLConnection) new URL(deleteUrl).openConnection();
        conn.setRequestMethod("DELETE");
        conn.setConnectTimeout(30000);

        int responseCode = conn.getResponseCode();
        return responseCode == 200;
    }

    /**
     * 上传文件到 HDFS
     */
    public boolean uploadFile(MultipartFile file) throws Exception {
        String fileName = file.getOriginalFilename();
        long fileSize = file.getSize();

        System.out.println("开始上传: " + fileName + ", 大小: " + fileSize + " bytes");

        // 1. 创建文件请求
        String createUrl = webHdfsUrl + "/food_data/" + fileName +
                "?op=CREATE&user.name=" + hdfsUser +
                "&overwrite=true&permission=777";

        System.out.println("CREATE请求: " + createUrl);

        HttpURLConnection conn = (HttpURLConnection) new URL(createUrl).openConnection();
        conn.setRequestMethod("PUT");
        conn.setInstanceFollowRedirects(false);
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(30000);

        int responseCode = conn.getResponseCode();
        System.out.println("CREATE响应码: " + responseCode);

        // 打印响应头（调试用）
        System.out.println("响应头:");
        conn.getHeaderFields().forEach((k, v) -> System.out.println("  " + k + ": " + v));

        if (responseCode == 307) {
            String location = conn.getHeaderField("Location");
            System.out.println("原始重定向地址: " + location);

            // 替换 localhost 为虚拟机 IP
            location = location.replace("localhost", "192.168.2.128");
            System.out.println("修改后重定向地址: " + location);

            // 2. 上传数据到 DataNode
            HttpURLConnection uploadConn = (HttpURLConnection) new URL(location).openConnection();
            uploadConn.setRequestMethod("PUT");
            uploadConn.setDoOutput(true);
            uploadConn.setConnectTimeout(90000);      // 增加连接超时
            uploadConn.setReadTimeout(90000);         // 增加读取超时
            uploadConn.setRequestProperty("Content-Type", "application/octet-stream");

            // 关键：禁用自动重定向
            uploadConn.setInstanceFollowRedirects(false);

            try (InputStream inputStream = file.getInputStream();
                 OutputStream outputStream = uploadConn.getOutputStream()) {
                byte[] buffer = new byte[16384];  // 增大缓冲区
                int bytesRead;
                long totalWritten = 0;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                    totalWritten += bytesRead;
                    System.out.println("已写入: " + totalWritten + " / " + fileSize + " bytes");
                }
                outputStream.flush();
                System.out.println("数据写入完成: " + totalWritten + " bytes");
            } catch (Exception e) {
                System.err.println("写入数据时出错: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }

            int uploadCode = uploadConn.getResponseCode();
            System.out.println("UPLOAD响应码: " + uploadCode);

            // 读取响应内容（如果有）
            try (InputStream is = uploadConn.getInputStream()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("响应: " + line);
                }
            } catch (Exception e) {
                // 可能没有响应体
            }

            return uploadCode == 201;

        } else if (responseCode == 201) {
            System.out.println("文件已存在且 overwrite=true");
            return true;
        } else {
            // 读取错误信息
            try (InputStream es = conn.getErrorStream();
                 BufferedReader reader = new BufferedReader(new InputStreamReader(es))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                System.err.println("错误信息: " + sb);
            } catch (Exception e) {
                // 忽略
            }
            return false;
        }
    }
}