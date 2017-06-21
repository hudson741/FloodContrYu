package com.yss.yarn.model;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/7
 */
public class DockerResource {

    // 每个容器的内存使用量
    private int memory;

    // 每个容器的cpu核数量
    private int cpuVitrualCores;

    public DockerResource( int memory, int cpuVitrualCores) {
        this.memory          = memory;
        this.cpuVitrualCores = cpuVitrualCores;
    }

    public static DockerResource newInstance( int memory, int cpuVitrualCores) {
        return new DockerResource(memory, cpuVitrualCores);
    }

    public int getCpuVitrualCores() {
        return cpuVitrualCores;
    }

    public void setCpuVitrualCores(int cpuVitrualCores) {
        this.cpuVitrualCores = cpuVitrualCores;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

}
