package com.floodCtr.job;


import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class FloodJob {

    // 发布任务id
    private String jobId;

    // cpu强制要求
    private int cpu;

    // memory强制要求
    private int memory;

    // docker 启动相关参数
    DockerCMD dockerCMD;

    // dockerjob运行中本地资源描述，通常指向hdfs
    Map<String, LocalResource> localResources;

    public FloodJob(String jobId, int cpu, int memory) {
        this.jobId  = jobId;
        this.cpu    = cpu;
        this.memory = memory;
    }

    public DockerCMD buildDockerCMD() {
        DockerCMD dockerCMD = new DockerCMD();
        this.dockerCMD = dockerCMD;
        return dockerCMD;
    }

    public FloodJob clone() {
        FloodJob floodJob = new FloodJob(this.jobId,this.cpu,this.memory);
        floodJob.setDockerCMD(this.dockerCMD);
        floodJob.setLocalResources(this.localResources);
        return floodJob;
    }

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }

    public DockerCMD getDockerCMD() {
        return dockerCMD;
    }

    public void setDockerCMD(DockerCMD dockerCMD) {
        this.dockerCMD = dockerCMD;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Map<String, LocalResource> getLocalResources() {
        return localResources;
    }

    public void setLocalResources(Map<String, LocalResource> localResources) {
        this.localResources = localResources;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    /**
     * 用于创建docker容器所需的配置
     */
    public static class DockerCMD {

        // docker host映射
        private Map<String, String> host = new HashMap<>();

        // port 宿主机映射
        private Map<String, String> port = new HashMap<>();

        // 使用镜像
        private String imageName;

        // 容器名称
        private String containerName;

        // 启动docker容器传入参数
        private String dockerArgs;

        // overlay IP
        private String ip;

        // hostName
        private String hostName;

        public DockerCMD containerName(String containerName) {
            this.containerName = containerName;

            return this;
        }

        public DockerCMD dockerArgs(String dockerArgs) {
            this.dockerArgs = dockerArgs;

            return this;
        }

        public DockerCMD host(String domainName, String ip) {
            this.host.put(domainName, ip);

            return this;
        }

        public DockerCMD hostName(String hostName) {
            this.hostName = hostName;

            return this;
        }

        public DockerCMD imageName(String imageName) {
            this.imageName = imageName;

            return this;
        }

        public DockerCMD ip(String ip) {
            this.ip = ip;

            return this;
        }

        public DockerCMD port(String dockerPort, String hostPort) {
            this.port.put(dockerPort, hostPort);

            return this;
        }

        public String getContainerName() {
            return containerName;
        }

        public String getDockerArgs() {
            return dockerArgs;
        }

        public Map<String, String> getHost() {
            return host;
        }

        public String getHostName() {
            return hostName;
        }

        public String getImageName() {
            return imageName;
        }

        public String getIp() {
            return ip;
        }

        public Map<String, String> getPort() {
            return port;
        }
    }
}
