package com.floodCtr;

import java.io.IOException;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floodCtr.publish.PRIORITY;

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

    private YarnClient(YarnConfiguration yarnConf) {
        nmClient = (NMClientImpl) NMClient.createNMClient();
        nmClient.init(yarnConf);
        nmClient.start();
        init(yarnConf);
        start();
    }

    static YarnClient yarnClient;

    public static YarnClient getInstance(){
        if(yarnClient!=null){
            return yarnClient;
        }

        yarnClient = new YarnClient(new YarnConfiguration());
        return yarnClient;
    }


    /**
     * 向yarn resourceManager申请资源
     * @param memory
     * @param cpu
     * @param nodes
     * @param racks
     * @param priority
     */
    public void addContainerRequest(int memory, int cpu, String[] nodes, String[] racks, PRIORITY priority) {
        Priority priority1 = Records.newRecord(Priority.class);

        priority1.setPriority(priority.getCode());

        AMRMClient.ContainerRequest req = new AMRMClient.ContainerRequest(Resource.newInstance(memory, cpu),
                                                                          nodes,
                                                                          racks,
                                                                          priority1);

        addContainerRequest(req);
        LOG.info("apply to the yarn to get memory:" + memory + " cpu:" + cpu);
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


}
