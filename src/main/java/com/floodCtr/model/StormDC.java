package com.yss.yarn.model;

import com.yss.yarn.StormDockerService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/12
 */
public class StormDC {

    ContainerId containerId;

    private NodeId nodeId;

    private StormDockerService.PROCESS process;

    public StormDC(ContainerId containerId,NodeId nodeId,StormDockerService.PROCESS process){
        this.containerId = containerId;
        this.nodeId = nodeId;
        this.process = process;
    }

    public ContainerId getContainerId() {
        return containerId;
    }

    public void setContainerId(ContainerId containerId) {
        this.containerId = containerId;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public void setNodeId(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    public StormDockerService.PROCESS getProcess() {
        return process;
    }

    public void setProcess(StormDockerService.PROCESS process) {
        this.process = process;
    }



}
