package com.floodCtr.publish;

import com.floodCtr.YarnClient;
import com.floodCtr.job.FloodJob;
import com.floodCtr.job.JobRegisterPubTable;
import com.floodCtr.monitor.FloodContrRunningMonitor;
import com.floodCtr.monitor.FloodJobRunningState;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class FloodContrJobPubProxy {

    public YarnClient yarnClient;

    public FloodContrJobPubProxy(YarnClient yarnClient){
        this.yarnClient = yarnClient;
    }

    private  final Logger LOG = LoggerFactory.getLogger(FloodContrJobPubProxy.class);

    /**
     * 发布任务,指定node时，可能为重启
     * @param floodContrJob
     * @param priority
     */
    public  void publishJob(FloodJob floodContrJob,String node, FloodJob.PRIORITY priority) {

        String[] nodes = null;
        if(StringUtils.isNotEmpty(node)){
            nodes = new String[1];
            nodes[0] = node;
        }

        // yarn资源申请
        LOG.info("containerrequest :"+floodContrJob.getJobId());
        yarnClient.addContainerRequest(floodContrJob.getMemory(), floodContrJob.getCpu(), nodes, null, priority);

        if(floodContrJob.getLaunch_type() == FloodJob.LAUNCH_TYPE.RESTART){
            FloodJobRunningState floodJobRunningState = FloodContrRunningMonitor.floodJobRunningStates.get(floodContrJob.getJobId());
            floodJobRunningState.setRunningState(FloodJobRunningState.RUNNING_STATE.RESTARTING);
        }else{
            registerToFloodJobMonitor(floodContrJob);
        }
        // 任务注册表完成注册
        LOG.info("job register :"+floodContrJob.getJobId());
        JobRegisterPubTable.registerJob(floodContrJob);

    }

    /**
     * 发布任务
     * @param floodContrJob
     * @param priority
     */
    public  void publishJob(FloodJob floodContrJob, FloodJob.PRIORITY priority) {

        // yarn资源申请
        LOG.info("containerrequest :"+floodContrJob.getJobId());
        yarnClient.addContainerRequest(floodContrJob.getMemory(), floodContrJob.getCpu(), null, null, priority);


        //任务监控队列完成注册
        registerToFloodJobMonitor(floodContrJob);
        // 任务注册表完成注册
        LOG.info("job register :"+floodContrJob.getJobId());
        JobRegisterPubTable.registerJob(floodContrJob);

    }

    /**
     * 注册任务到监控队列
     * @param floodJob
     */
    public void registerToFloodJobMonitor(FloodJob floodJob){
        String               businessType         =
                StringUtils.isNotEmpty(System.getenv("appName"))
                        ? System.getenv("appName")
                        : "default";
        FloodJobRunningState floodJobRunningState =
                new FloodJobRunningState(floodJob.getJobId(),null,
                        null,
                        businessType);

        floodJobRunningState.setFloodJob(floodJob.clone());
        //任务监听注册表完成注册
        FloodContrRunningMonitor.registerJob(floodJobRunningState);
    }
}
