package com.floodCtr;

import java.io.IOException;

import java.util.List;
import java.util.Map;

import com.floodCtr.job.FloodJob;
import com.floodCtr.monitor.FloodJobRunningState;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.collect.Lists;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class YarnClient extends AMRMClientImpl<AMRMClient.ContainerRequest>{
    private static final Logger LOG = LoggerFactory.getLogger(YarnClient.class);

    // 与nodemanager交互
    private NMClientImpl nmClient;

    private org.apache.hadoop.yarn.client.api.YarnClient yarnClient1;

    private YarnClient(YarnConfiguration yarnConf) {
        nmClient = (NMClientImpl) NMClient.createNMClient();
        nmClient.init(yarnConf);
        nmClient.cleanupRunningContainersOnStop(true);
        nmClient.start();
        init(yarnConf);
        start();

        yarnClient1 = YarnClientImpl.createYarnClient();
        yarnClient1.init(yarnConf);
        yarnClient1.start();
    }

    public org.apache.hadoop.yarn.client.api.YarnClient getYarnClient1() {
        return yarnClient1;
    }

    public void setYarnClient1(org.apache.hadoop.yarn.client.api.YarnClient yarnClient1) {
        this.yarnClient1 = yarnClient1;
    }

    static YarnClient yarnClient;

    public static YarnClient getInstance(){
        if(yarnClient!=null){
            return yarnClient;
        }

        yarnClient = new YarnClient(new YarnConfiguration());
        return yarnClient;
    }

    public void stop(){
        try {
            serviceStop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 向yarn resourceManager申请资源
     * @param memory
     * @param cpu
     * @param nodes
     * @param racks
     * @param priority
     */
    public void addContainerRequest(int memory, int cpu, String[] nodes, String[] racks, FloodJob.PRIORITY priority) {
        Priority priority1 = Priority.newInstance(priority.getCode());

        AMRMClient.ContainerRequest req = new AMRMClient.ContainerRequest(Resource.newInstance(memory, cpu),
                                                                          nodes,
                                                                          racks,
                                                                              priority1, (nodes == null || nodes.length == 0)?true:false);


        addContainerRequest(req);
        LOG.info("apply to the yarn to get memory:" + memory + " cpu:" + cpu);
    }

    public List<String> getNodes(){

        List<String> nodes = Lists.newArrayList();
        try {
            List<NodeReport> nodeIds = yarnClient1.getNodeReports(NodeState.RUNNING);
            LOG.info("fuck is here  ...  "+nodeIds.size());

            for(NodeReport nodeReport:nodeIds){
                nodes.add(nodeReport.getNodeId().getHost());
            }
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return nodes;

    }


    /**
     * 向resoursemanager确认资源的获取
     * @return
     */
    public  List<Container> allocateContainers() {
        try {
            AllocateResponse allocResponse       = allocate(0.5f);
            List<Container>  allocatedContainers = allocResponse.getAllocatedContainers();

            return allocatedContainers;
        } catch (YarnException e) {
            LOG.error("error ,", e);
        } catch (IOException e) {
            LOG.error("error ,", e);
        }

        return null;
    }

    /**
     * 资源释放
     * @param floodJobRunningState
     */
    public void stopContainer(FloodJobRunningState floodJobRunningState) {

        try {

            if(floodJobRunningState.getRunningState() == FloodJobRunningState.RUNNING_STATE.RUNNING) {
                this.nmClient.stopContainer(floodJobRunningState.getContainerId(), floodJobRunningState.getNodeId());
            }
            releaseAssignedContainer(floodJobRunningState.getContainerId());
        } catch (Throwable e) {
            LOG.error("error ",e);
        }

    }

    /**
     * 资源释放
     * @param containerId
     */
    public void releaseContainer(ContainerId containerId) {
        releaseAssignedContainer(containerId);

    }

    /**
     * 提交任务至yarn
     * @param container
     * @param localResource
     * @param yarnCommond
     * @return
     */
    public int startDockerContainer(Container container, Map<String, LocalResource> localResource,
                                           String yarnCommond) {
        ContainerLaunchContext launchContext = ContainerLaunchContext.newInstance(localResource,
                                                                                  null,
                                                                                  Lists.newArrayList(yarnCommond),
                                                                                  null,
                                                                                  null,
                                                                                  null);

        try {
            nmClient.startContainer(container, launchContext);
            return 1;
        } catch (YarnException e) {
            LOG.error("Caught an exception while trying to start a container", e);
        } catch (IOException e) {
            LOG.error("Caught an exception while trying to start a container", e);
        }

        return 0;
    }

    public NMClientImpl getNmClient() {
        return nmClient;
    }

}
