package com.yss.yarn;


import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yss.yarn.model.DockerResource;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/7
 */
public class RequestRMClient {
    private static final Logger    LOG              = LoggerFactory.getLogger(RequestRMClient.class);
    private static RequestRMClient requestRMClient  = new RequestRMClient();
    private final Priority         DEFAULT_PRIORITY = Records.newRecord(Priority.class);

    private RequestRMClient() {
        Integer pri = 0;

        this.DEFAULT_PRIORITY.setPriority(pri);
    }

    public void dockerResourceRequest(StormAMRMClient stormAMRMClient, DockerResource dockerResource,
                                      StormDockerService.PROCESS process) {
        if (dockerResource == null) {
            return;
        }

        LOG.info("request 1 docker container");

        AMRMClient.ContainerRequest req =
            new AMRMClient.ContainerRequest(Resource.newInstance(dockerResource.getMemory(),
                                                                 dockerResource.getCpuVitrualCores()),
                                            null,    // String[] nodes,
                                            null,    // String[] racks,
                                            DEFAULT_PRIORITY);

        stormAMRMClient.addContainerRequest(req);

        StormRunningState.getInstance().increaseProcessExceptRunNum(process,1);
    }

    public static RequestRMClient getInstance() {
        return requestRMClient;
    }
}
