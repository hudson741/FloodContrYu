package com.floodCtr.publish;

import com.floodCtr.YarnClient;
import com.floodCtr.job.FloodJob;
import com.floodCtr.job.JobRegisterPubTable;
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
     * 发布任务
     * @param floodContrJob
     * @param priority
     */
    public  void publishJob(FloodJob floodContrJob, PRIORITY priority) {

        // yarn资源申请
        LOG.info("containerrequest :"+floodContrJob.getJobId());
        yarnClient.addContainerRequest(floodContrJob.getMemory(), floodContrJob.getCpu(), null, null, priority);

        // 任务注册表完成注册
        LOG.info("job register :"+floodContrJob.getJobId());
        JobRegisterPubTable.registerJob(floodContrJob);
    }
}
