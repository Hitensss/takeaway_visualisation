package org.example.takeaway_springboot.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.core.io.ClassPathResource;

import java.net.URI;

@Component
public class HdfsConfig {

    @Value("${hdfs.uri}")
    private String hdfsUri;

    @Value("${hdfs.user}")
    private String hdfsUser;

    @Bean
    public FileSystem fileSystem() {
        try {
            Configuration conf = new Configuration();

            // 自动加载 classpath 下的 core-site.xml 和 hdfs-site.xml
            conf.addResource(new org.apache.hadoop.fs.Path("core-site.xml"));
            conf.addResource(new org.apache.hadoop.fs.Path("hdfs-site.xml"));

            // 覆盖 URI
            conf.set("fs.defaultFS", hdfsUri);
            conf.set("dfs.client.use.datanode.hostname", "true");

            FileSystem fs = FileSystem.get(new URI(hdfsUri), conf, hdfsUser);

            System.out.println("HDFS 连接成功: " + hdfsUri);
            return fs;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}