package com.yss.yarn.resource;

import com.yss.yarn.StormAMRMClient;
import com.yss.yarn.rpc.MasterServer;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/7
 */
public class ContainerLaunchQueue {

    private static final Logger LOG = LoggerFactory.getLogger(MasterServer.class);


    public final static BlockingQueue<Container> launcherQueue = new LinkedBlockingQueue<>();


    public static void circleAllocateContainer(final StormAMRMClient client,
                               final int heartBeatIntervalMs){

        Thread thread = new Thread() {
            @Override
            public void run() {
                    while ((client.getServiceState() == Service.STATE.STARTED)
                            &&!Thread.currentThread().isInterrupted()) {

                        try {
                            Thread.sleep(heartBeatIntervalMs);

                            /**
                             * We always send 50% progress.
                             *
                             * 不断获取resourcemanager分配好的资源，这里不是申请，而是获取已经分配了的
                             *
                             */
                            AllocateResponse allocResponse = client.allocate(0.5f);

                            List<Container> allocatedContainers = allocResponse.getAllocatedContainers();

                            if (allocatedContainers.size() > 0) {
                                LOG.info("allocated  "+allocatedContainers.size() +" and add to local ");
                                launcherQueue.addAll(allocatedContainers);
                            }
                        }catch (Throwable t) {
                            LOG.error("Unhandled error in AM: ", t);
                        }
                    }
            }
        };

        thread.start();

    }



}
