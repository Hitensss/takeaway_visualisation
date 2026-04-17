package org.example.takeaway_springboot.controller;

import org.example.takeaway_springboot.dto.ApiResponse;
import org.example.takeaway_springboot.service.WebHdfsService;
import org.example.takeaway_springboot.service.SshService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin")
@CrossOrigin(origins = "*")
public class AdminController {

    @Autowired
    private WebHdfsService hdfsService;

    @Autowired
    private SshService sshService;

    /**
     * 获取 HDFS 文件列表
     */
    @GetMapping("/files")
    public ApiResponse<List<Map<String, Object>>> listFiles() {
        try {
            List<Map<String, Object>> files = hdfsService.listFiles();
            return ApiResponse.success(files);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiResponse.error("获取文件列表失败: " + e.getMessage());
        }
    }

    /**
     * 上传文件并触发 Spark 分析
     */
    @PostMapping("/upload")
    public ApiResponse<Map<String, String>> uploadAndAnalyze(@RequestParam("file") MultipartFile file) {
        try {
            String fileName = file.getOriginalFilename();

            // 1. 上传到 HDFS
            boolean uploadSuccess = hdfsService.uploadFile(file);
            if (!uploadSuccess) {
                return ApiResponse.error("文件上传失败");
            }

            // 2. 通过 SSH 调用虚拟机上的 spark-submit
            boolean analyzeSuccess = sshService.executeSparkSubmit(fileName);

            Map<String, String> result = new HashMap<>();
            result.put("fileName", fileName);
            result.put("uploadStatus", "success");
            result.put("analyzeStatus", analyzeSuccess ? "success" : "failed");

            return ApiResponse.success(result);

        } catch (Exception e) {
            return ApiResponse.error("操作失败: " + e.getMessage());
        }
    }

    /**
     * 删除 HDFS 文件
     */
    @DeleteMapping("/files/{fileName}")
    public ApiResponse<String> deleteFile(@PathVariable String fileName) {
        try {
            boolean success = hdfsService.deleteFile(fileName);
            return success ? ApiResponse.success("删除成功") : ApiResponse.error("删除失败");
        } catch (Exception e) {
            return ApiResponse.error("删除失败: " + e.getMessage());
        }
    }
}