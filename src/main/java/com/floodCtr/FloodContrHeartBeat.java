package com.floodCtr;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floodCtr.Resources.FloodContrContainerPool;
import com.floodCtr.rpc.FloodContrMaster;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class FloodContrHeartBeat {
    private final Logger LOG = LoggerFactory.getLogger(FloodContrMaster.class);
    private YarnClient   yarnClient;

    public FloodContrHeartBeat(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }

    /**
     * 轮询心跳+资源获取
     */
    public void scheduleHeartBeat(final int period, final TimeUnit timeUnit) {
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {

//                      LOG.info("floodinfo : alocate container .....");
                        Thread.currentThread().sleep(1000);

                        if ((yarnClient.getServiceState() == Service.STATE.STARTED)
                                &&!Thread.currentThread().isInterrupted()) {
                            List<Container> containers = yarnClient.allocateContainers();

                            if (CollectionUtils.isNotEmpty(containers)) {
                                LOG.info("floodinfo : container is taken and add to pool....");
                                FloodContrContainerPool.containerPool.addAll(containers);
                            }
                        } else {
                            LOG.info("yarn service not start ");
                        }
                    }
                } catch (Throwable t) {
                    LOG.error("error ,", t);
                }
            }
        };

        thread.start();
    }
}
