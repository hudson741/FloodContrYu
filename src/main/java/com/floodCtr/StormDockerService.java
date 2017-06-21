package com.yss.yarn;

import java.io.IOException;

import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yss.yarn.resource.IPManager;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/7
 */
public class StormDockerService {
    private static final Logger LOG       = LoggerFactory.getLogger(StormDockerService.class);
    private static IPManager    ipManager = IPManager.getInstance();

    public enum PROCESS {
        NIMBUS("nimbus"), UI("ui"), SUPERVISOR("supervisor");

        private String code;

        PROCESS(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }
    }

    public static void circleStormDockerLaunch(final StormAMRMClient client,
                                               final BlockingQueue<Container> launcherQueue, final PROCESS process) {
        Thread thread = new Thread() {
            Container container;
            @Override
            public void run() {
                while ((client.getServiceState() == Service.STATE.STARTED) &&!Thread.currentThread().isInterrupted()) {
                    try {
                        StormRunningState stormRunningState = StormRunningState.getInstance();

                        if (stormRunningState.isProcessExpectRun(process)) {
                            LOG.info(process.getCode() + " is expect to run...");
                            container = launcherQueue.take();
                            LOG.info("launcher : taking container with id " + container.getId() + " from the queue");

                            String dockerIp = IPManager.getInstance().getIPByProcess(process);

                            client.launchStormComponentOnDocker(container, process, dockerIp);
                            stormRunningState.increaseProcessRunningState(process,1);
                        }

                    } catch (InterruptedException e) {
//                        if (client.getServiceState() == Service.STATE.STARTED) {
                            LOG.error("Launcher thread interrupted : ", e);
//                            System.exit(1);
//                        }

                        return;
                    } catch (IOException e) {
                        LOG.error("Launcher thread I/O exception : ", e);
//                        System.exit(1);
                    }
                }
            }
        };

        thread.start();
    }
}
