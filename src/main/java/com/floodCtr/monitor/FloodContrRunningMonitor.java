package com.floodCtr.monitor;

import com.floodCtr.YarnClient;
import com.floodCtr.job.FloodJob;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/26
 */
public class FloodContrRunningMonitor {

    private static final Logger logger            = LoggerFactory.getLogger(FloodContrRunningMonitor.class);

    public static volatile ConcurrentHashMap<String, FloodJobRunningState> floodJobRunningStates = new ConcurrentHashMap<>();

    private YarnClient yarnClient;

    public FloodContrRunningMonitor(YarnClient yarnClient){
        this.yarnClient = yarnClient;
    }

    public static List<FloodJobRunningState> getFloodJobRunningState(){
        List<FloodJobRunningState> result = Lists.newArrayList();
        Collection collection = floodJobRunningStates.values();
        if(CollectionUtils.isEmpty(collection)){
            return result;
        }else{
            Iterator iterator = collection.iterator();
            while(iterator.hasNext()){
                result.add((FloodJobRunningState)iterator.next());
            }
        }
        return result;
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

    public void monitor(){
        Thread thread = new Thread(){

            @Override
            public void run(){
              ContainerId containerId = null;
                while (true){
                    try{
                        Thread.currentThread().sleep(1000);
                        if(floodJobRunningStates.isEmpty()){
                            logger.info("floodJobRunningStates is empty");
                        }else{
                            for(FloodJobRunningState floodJobRunningState: floodJobRunningStates.values()){
                                try {
                                    containerId = floodJobRunningState.getContainerId();
                                    if(floodJobRunningState.getRunningState() == FloodJobRunningState.RUNNING_STATE.RUNNING) {
                                        ContainerStatus containerStatus = yarnClient.getNmClient().getContainerStatus(floodJobRunningState.getContainerId(), floodJobRunningState.getNodeId());
                                        logger.info("size  containerStatus" + floodJobRunningStates.size() + "  with containerState " + containerStatus.getState() + " containerId"
                                                + containerStatus.getContainerId() + " status " + containerStatus.getExitStatus());
                                    }
                                }catch(Exception e){
                                    logger.info("container not exits ",e);
                                    yarnClient.releaseContainer(containerId);
                                    floodJobRunningState.setRunningState(FloodJobRunningState.RUNNING_STATE.STOP);

                                }
                            }
                        }



                    }catch(Exception e){
                        logger.info("error ",e);
                    }
                }
            }

        };
        thread.start();


    }


}
