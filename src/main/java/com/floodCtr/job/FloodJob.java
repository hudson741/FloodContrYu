package com.floodCtr.job;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class FloodJob implements Serializable {

    // 任务紧急度
    private PRIORITY priority = PRIORITY.DEFAULT_PRIORITY;

    // 1:新建  2:重启  默认都是新建
    private LAUNCH_TYPE launch_type = LAUNCH_TYPE.NEW;

    // 发布任务id
    private String jobId;

    // cpu强制要求
    private int cpu;

    // memory强制要求
    private int memory;

    // docker网络
    private String netUrl;

    // docker 启动相关参数
    DockerCMD dockerCMD;

    // 是否制定IP地址运行
    private String nodeBind;

    // 业务标识
    private String businessTag;

    // dockerjob运行中本地资源描述，通常指向hdfs
    Map<String, LocalResource> localResources;

    public FloodJob() {}

    public FloodJob(String jobId, CM cm) {
        this.jobId  = jobId;
        this.cpu    = cm.getCpu();
        this.memory = cm.getMemory();
    }

    public FloodJob(String jobId, int cpu, int memory) {
        this.jobId  = jobId;
        this.cpu    = cpu;
        this.memory = memory;
    }

    public enum CM {
        CMLOW(1, 1024), CMLOW2(1, 1024), CMNOMAL(1, 2048), CMH1(2, 4096), CMH2(4, 4096);

        private int cpu;
        private int memory;

        CM(int cpu, int memory) {
            this.cpu    = cpu;
            this.memory = memory;
        }

        public static CM getCM(String cm) {
            if (StringUtils.isEmpty(cm)) {
                return CMNOMAL;
            }

            int code = 1;

            try {
                code = Integer.parseInt(cm);
            } catch (Throwable e) {}

            if (code == 0) {
                return CMLOW2;
            } else if (code == 1) {
                return CMNOMAL;
            } else if (code == 2) {
                return CMH1;
            } else if (code == 3) {
                return CMH2;
            } else if (code == 4) {
                return CMLOW;
            } else {
                return CMNOMAL;
            }
        }

        public int getCpu() {
            return cpu;
        }

        public void setCpu(int cpu) {
            this.cpu = cpu;
        }

        public int getMemory() {
            return memory;
        }

        public void setMemory(int memory) {
            this.memory = memory;
        }
    }

    /**
     * 任务类型，新建，还是重启
     */
    public enum LAUNCH_TYPE {
        NEW(1), RESTART(2);

        private int code;

        private LAUNCH_TYPE() {}

        ;
        LAUNCH_TYPE(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }
    }

    /**
     * 描述任务发布的紧急程度
     */
    public enum PRIORITY {
        LOW(2), DEFAULT_PRIORITY(1), HIGH(0);

        private int code;

        PRIORITY(int code) {
            this.code = code;
        }

        public static PRIORITY getByCode(int code) {
            if (LOW.getCode() == code) {
                return LOW;
            } else if (HIGH.getCode() == code) {
                return HIGH;
            } else {
                return DEFAULT_PRIORITY;
            }
        }

        public static PRIORITY getByCodeStr(String codeStr) {
            if (StringUtils.isEmpty(codeStr)) {
                return PRIORITY.DEFAULT_PRIORITY;
            } else {
                int code = Integer.parseInt(codeStr);

                return getByCode(code);
            }
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }
    }

    public DockerCMD buildDockerCMD() {
        DockerCMD dockerCMD = new DockerCMD();

        this.dockerCMD = dockerCMD;

        return dockerCMD;
    }

    public FloodJob businessTag(String businessTag) {
        this.businessTag = businessTag;

        return this;
    }

    public FloodJob clone() {
        return new FloodJob(this.jobId, this.cpu, this.memory).dockerCMD(this.dockerCMD)
                                                              .nodeBind(this.getNodeBind())
                                                              .netUrl(this.getNetUrl())
                                                              .businessTag(this.getBusinessTag())
                                                              .localResources(this.getLocalResources())
                                                              .priority(this.getPriority())
                                                              .launch_type(this.getLaunch_type());
    }

    public FloodJob cpu(int cpu) {
        this.cpu = cpu;

        return this;
    }

    public FloodJob dockerCMD(DockerCMD dockerCMD) {
        this.dockerCMD = dockerCMD;

        return this;
    }

    public FloodJob jobId(String jobId) {
        this.jobId = jobId;

        return this;
    }

    public FloodJob launch_type(LAUNCH_TYPE launch_type) {
        this.launch_type = launch_type;

        return this;
    }

    public FloodJob localResources(Map<String, LocalResource> localResources) {
        this.localResources = localResources;

        return this;
    }

    public FloodJob netUrl(String netUrl) {
        this.netUrl = netUrl;

        return this;
    }

    public FloodJob nodeBind(String nodeBind) {
        this.nodeBind = nodeBind;

        return this;
    }

    public FloodJob priority(PRIORITY priority) {
        this.priority = priority;

        return this;
    }

    public String getBusinessTag() {
        return businessTag;
    }

    public void setBusinessTag(String businessTag) {
        this.businessTag = businessTag;
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

    public LAUNCH_TYPE getLaunch_type() {
        return launch_type;
    }

    public void setLaunch_type(LAUNCH_TYPE launch_type) {
        this.launch_type = launch_type;
    }

    @JSONField(serialize = false)
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

    public String getNetUrl() {
        return netUrl;
    }

    public void setNetUrl(String netUrl) {
        this.netUrl = netUrl;
    }

    public String getNodeBind() {
        return nodeBind;
    }

    public void setNodeBind(String nodeBind) {
        this.nodeBind = nodeBind;
    }

    public PRIORITY getPriority() {
        return priority;
    }

    public void setPriority(PRIORITY priority) {
        this.priority = priority;
    }

    /**
     * 用于创建docker容器所需的配置
     */
    public static class DockerCMD implements Serializable {

        // docker host映射
        private Map<String, String> host = new HashMap<>();

        // port 宿主机映射
        private Map<String, String> port = new HashMap<>();

        // volume目录对应
        private Map<String, String> volume = new HashMap<>();

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

        public DockerCMD hosts(Map<String, String> hostMap) {
            if (MapUtils.isNotEmpty(hostMap)) {
                for (String domainName : hostMap.keySet()) {
                    this.host.put(domainName, hostMap.get(domainName));
                }
            }

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

        public DockerCMD ports(Map<String, String> portMap) {
            if (MapUtils.isNotEmpty(portMap)) {
                for (String dockerPort : portMap.keySet()) {
                    this.port.put(dockerPort, portMap.get(dockerPort));
                }
            }

            return this;
        }

        public DockerCMD volume(String localDir, String dockerDir) {
            this.volume.put(localDir, dockerDir);

            return this;
        }

        public String getContainerName() {
            return containerName;
        }

        public void setContainerName(String containerName) {
            this.containerName = containerName;
        }

        public String getDockerArgs() {
            return dockerArgs;
        }

        public void setDockerArgs(String dockerArgs) {
            this.dockerArgs = dockerArgs;
        }

        public Map<String, String> getHost() {
            return host;
        }

        public void setHost(Map<String, String> host) {
            this.host = host;
        }

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        public String getImageName() {
            return imageName;
        }

        public void setImageName(String imageName) {
            this.imageName = imageName;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public Map<String, String> getPort() {
            return port;
        }

        public void setPort(Map<String, String> port) {
            this.port = port;
        }

        public Map<String, String> getVolume() {
            return volume;
        }

        public void setVolume(Map<String, String> volume) {
            this.volume = volume;
        }
    }
}
