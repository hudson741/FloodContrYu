package com.floodCtr.rpc;

import java.io.IOException;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import com.floodCtr.YarnClient;
import com.floodCtr.generate.FloodContrThriftService;
import com.floodCtr.job.FloodJob;
import com.floodCtr.monitor.FloodContrRunningMonitor;
import com.floodCtr.monitor.FloodJobRunningState;
import com.floodCtr.publish.FloodContrJobPubProxy;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/7/2
 */
public class FloodContrThriftServiceImpl implements FloodContrThriftService.Iface {
    private static final Logger     logger = LoggerFactory.getLogger(FloodContrThriftServiceImpl.class);
    protected YarnClient            yarnClient;
    protected FloodContrJobPubProxy floodContrJobPubProxy;

    public FloodContrThriftServiceImpl(YarnClient yarnClient, FloodContrJobPubProxy floodContrJobPubProxy) {
        this.yarnClient            = yarnClient;
        this.floodContrJobPubProxy = floodContrJobPubProxy;
    }

    @Override
    public String killApplication(String appId) throws TException {

        try {

            for(FloodJobRunningState floodJobRunningState:  FloodContrRunningMonitor.floodJobRunningStates.values()){
                yarnClient.stopContainer(floodJobRunningState);
            }
            yarnClient.stop();

            yarnClient.getYarnClient1().killApplication(ApplicationId.fromString(appId));

        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "killing";
    }

    @Override
    public String restartDocker(String jobId) throws TException {
        FloodJobRunningState floodJobRunningState = FloodContrRunningMonitor.floodJobRunningStates.get(jobId);

        if (floodJobRunningState == null) {
            return "任务已不存在";
        } else if (floodJobRunningState.getRunningState() == FloodJobRunningState.RUNNING_STATE.RESTARTING) {
            return "正在重启，无需再重启";
        }

        FloodJob floodJob = floodJobRunningState.getFloodJob().clone();

        floodJob.launch_type(FloodJob.LAUNCH_TYPE.RESTART)
                .nodeBind(floodJobRunningState.getRunIp())
                .priority(FloodJob.PRIORITY.HIGH);
        floodJobRunningState.setFloodJob(floodJob.clone());
        floodContrJobPubProxy.publishJob(floodJob, floodJobRunningState.getRunIp(), FloodJob.PRIORITY.HIGH);

        return "已重新发布 " + floodJob.getJobId();
    }

    @Override
    public String stopDocker(String jobId) throws TException {
        FloodJobRunningState floodJobRunningState = FloodContrRunningMonitor.floodJobRunningStates.get(jobId);

        if ((floodJobRunningState == null)
                || (floodJobRunningState.getRunningState() != FloodJobRunningState.RUNNING_STATE.RUNNING)) {
            return "已停止，无需再停止";
        }

        yarnClient.stopContainer(floodJobRunningState);

        return "已处理";
    }

    @Override
    public String getAllDockerJob() throws TException {
        return JSONObject.toJSONString(FloodContrRunningMonitor.getFloodJobRunningState());
    }

    @Override
    public void addDockerComponent(String imageName, String containerName, String runIp, String dockerIp, String businessTag, String priority, String dockerArgs, String netUrl, Map<String, String> host, Map<String, String> port) throws TException {
        String jobId = UUID.randomUUID().toString();
        String cm    = System.getenv("cm");

        logger.info("cm : " + cm);

        if (StringUtils.isEmpty(cm)) {
            cm = "1";
        }

        int      cmi      = Integer.parseInt(cm);
        FloodJob floodJob = new FloodJob(jobId, 1 * cmi, 1024 * cmi);
        String   netUrl1  = System.getenv("netUrl");

        logger.info("add docker with args "+dockerArgs);

        logger.info("netUrl : " + netUrl1);
        floodJob.netUrl(netUrl ==null?netUrl1:netUrl);
        if(StringUtils.isNotEmpty(businessTag)){
            floodJob.businessTag(businessTag);
        }
        FloodJob.PRIORITY priority1 = FloodJob.PRIORITY.getByCodeStr(priority);
        floodJob.priority(priority1);
        floodJob.buildDockerCMD()
                .imageName(imageName)
                .containerName(containerName)
                .hostName(containerName)
                .host(containerName, dockerIp)
                .ip(dockerIp)
                .hosts(host)
                .ports(port)
                .dockerArgs(dockerArgs);
        floodContrJobPubProxy.publishJob(floodJob,runIp, priority1);
    }
}
