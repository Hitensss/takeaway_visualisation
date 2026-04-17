package org.example.takeaway_springboot.controller;

import org.example.takeaway_springboot.dto.ApiResponse;
import org.example.takeaway_springboot.service.SshService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/admin")
@CrossOrigin(origins = "*")
public class TaskStatusController {

    @Autowired
    private SshService sshService;

    /**
     * 获取所有Spark任务状态
     */
    @GetMapping("/tasks")
    public ApiResponse<Map<String, SshService.SparkTaskInfo>> getTaskStatus() {
        try {
            return ApiResponse.success(sshService.getAllTaskStatus());
        } catch (Exception e) {
            return ApiResponse.error("获取任务状态失败: " + e.getMessage());
        }
    }
}