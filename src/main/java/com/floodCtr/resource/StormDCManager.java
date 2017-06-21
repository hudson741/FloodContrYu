package com.yss.yarn.resource;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.yss.yarn.RequestRMClient;
import com.yss.yarn.StormAMRMClient;
import com.yss.yarn.StormDockerService;
import com.yss.yarn.model.DockerResource;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yss.yarn.model.StormDC;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/12
 */
public class StormDCManager {
    private static final Logger   LOG            = LoggerFactory.getLogger(StormDCManager.class);
    private static StormDCManager stormDCManager = new StormDCManager();
    public static volatile ConcurrentLinkedQueue<StormDC> list           = new ConcurrentLinkedQueue();

    public void circleStormContainerStatusCheck(final NMClientImpl nmClient,final StormAMRMClient stormAMRMClient) {
        Thread thread = new Thread() {

            @Override
            public void run(){
                while(true){
                    try {
                        Thread.currentThread().sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if(list.isEmpty()){
                        LOG.info("fuck ConcurrentLinkedQueue is empty");
                    }
                    for (StormDC stormDC : list) {
                        try {
                            ContainerStatus containerStatus = nmClient.getContainerStatus(stormDC.getContainerId(),
                                    stormDC.getNodeId());

                            LOG.info("fuck containerStatus : containerState " + containerStatus.getState() + " containerId"
                                    + containerStatus.getContainerId() + " status " + containerStatus.getExitStatus());

                        } catch (Exception e) {
                            //经过测试，容器运行失败，会自动回收，此处会报异常，情况为container已不存在
//                            reload(stormAMRMClient,stormDC);
                        }
                    }
                }
            }

        };

        thread.start();


    }

    private void reload(StormAMRMClient stormAMRMClient,StormDC stormDC){
        StormDockerService.PROCESS process = stormDC.getProcess();
        list.remove(stormDC);
        RequestRMClient.getInstance().dockerResourceRequest(stormAMRMClient, DockerResource.newInstance(2000,1),process);
    }

    public static StormDCManager getInstance() {
        return stormDCManager;
    }

    public ConcurrentLinkedQueue<StormDC> getList() {
        return list;
    }
}
