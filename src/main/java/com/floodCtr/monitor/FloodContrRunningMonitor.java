package com.floodCtr.monitor;

import java.io.IOException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import com.floodCtr.YarnClient;

import com.google.common.collect.Lists;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/26
 */
public class FloodContrRunningMonitor {
    private static final Logger                                            logger                =
        LoggerFactory.getLogger(FloodContrRunningMonitor.class);
    public static volatile ConcurrentHashMap<String, FloodJobRunningState> floodJobRunningStates =
        new ConcurrentHashMap<>();
    private YarnClient yarnClient;

    public FloodContrRunningMonitor(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }

    public void monitor() throws IOException {
        String            appId          = System.getenv("appId");
        YarnConfiguration yarnConf       = new YarnConfiguration();
        final FileSystem  fs             = FileSystem.get(yarnConf);
        final Path        floodStorePath = new Path(fs.getHomeDirectory(),
                                                    "store" + Path.SEPARATOR + appId + Path.SEPARATOR + "flood.txt");
        Thread saveT = new Thread() {
            public void run() {
                while (true) {
                    try {
                        Thread.currentThread().sleep(2000);

                        if (!floodJobRunningStates.isEmpty()) {
                            List<FloodJobRunningState> list   = getFloodJobRunningState();
                            Path                       dirDst = floodStorePath.getParent();

                            fs.mkdirs(dirDst);

                            FSDataOutputStream out  = fs.create(floodStorePath);
                            String             json = JSONObject.toJSONString(list);

                            out.write(json.getBytes("UTF-8"));
                            out.flush();
                            out.close();
                        }
                    } catch (Exception e) {
                        logger.error("error ", e);
                    }
                }
            }
        };

        saveT.start();

        Thread thread = new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.currentThread().sleep(1000);

                        if (!floodJobRunningStates.isEmpty()) {
                            for (FloodJobRunningState floodJobRunningState : floodJobRunningStates.values()) {
                                if (floodJobRunningState.getRunningState()
                                        == FloodJobRunningState.RUNNING_STATE.RUNNING) {
                                    try {
                                        ContainerStatus containerStatus = yarnClient.getNmClient()
                                                                                    .getContainerStatus(
                                                                                        ContainerId.fromString(
                                                                                            floodJobRunningState.getContainerIdStr()),
                                                                                        NodeId.newInstance(
                                                                                            floodJobRunningState.getNodeHOST(),
                                                                                            floodJobRunningState.getNodePort()));

                                        if (containerStatus.getState() == ContainerState.COMPLETE) {
                                            floodJobRunningState.setRunningState(
                                                FloodJobRunningState.RUNNING_STATE.STOP);
                                        }

//                                      logger.info("size  containerStatus" + floodJobRunningStates.size()
//                                              + "  with containerState " + containerStatus.getState()
//                                              + " containerId" + containerStatus.getContainerId() + " status "
//                                              + containerStatus.getExitStatus());
                                    } catch (Exception e) {
                                        logger.info("container not exits ", e);
                                        floodJobRunningState.setRunningState(FloodJobRunningState.RUNNING_STATE.STOP);
                                    } catch (Throwable e) {
                                        logger.info("nodemanager is died ", e);
                                        floodJobRunningState.setRunningState(FloodJobRunningState.RUNNING_STATE.STOP);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.info("error ", e);
                    }
                }
            }
        };

        thread.start();
    }

    /**
     * 注册任务至监控池
     * @param floodJobRunningState
     */
    public static synchronized void registerJob(FloodJobRunningState floodJobRunningState) {
        floodJobRunningStates.put(floodJobRunningState.getFloodJob().getJobId(), floodJobRunningState);
    }

    /**
     * 移除任务出监控池
     * @param id
     */
    public static synchronized void removeJob(String id) {
        floodJobRunningStates.remove(id);
    }

    public static List<FloodJobRunningState> getFloodJobRunningState() {
        List<FloodJobRunningState> result     = Lists.newArrayList();
        Collection                 collection = floodJobRunningStates.values();

        if (CollectionUtils.isEmpty(collection)) {
            return result;
        } else {
            Iterator iterator = collection.iterator();

            while (iterator.hasNext()) {
                result.add((FloodJobRunningState) iterator.next());
            }
        }

        return result;
    }
}
