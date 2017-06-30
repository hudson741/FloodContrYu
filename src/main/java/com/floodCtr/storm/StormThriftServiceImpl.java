package com.floodCtr.storm;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import com.floodCtr.generate.FloodContrThriftService;
import com.floodCtr.job.FloodJob;
import com.floodCtr.monitor.FloodContrRunningMonitor;
import com.floodCtr.monitor.FloodJobRunningState;
import com.floodCtr.publish.FloodContrJobPubProxy;
import com.google.common.collect.Lists;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/28
 */
public class StormThriftServiceImpl implements FloodContrThriftService.Iface {
    private static final Logger   logger = LoggerFactory.getLogger(StormThriftServiceImpl.class);
    private FloodContrJobPubProxy floodContrJobPubProxy;

    public StormThriftServiceImpl(FloodContrJobPubProxy floodContrJobPubProxy) {
        this.floodContrJobPubProxy = floodContrJobPubProxy;
    }

    @Override
    public void addDockerComponent(String imageName, String containerName, String dockerIp, String dockerArgs,
                                   String netUrl, Map<String, String> host, Map<String, String> port)
            throws TException {

        /**
         * 用于做初始化提交，也可以不写，当MasterServer启动后，通过thrift交互提交。
         */
        String jobId = UUID.randomUUID().toString();
        String cm    = System.getenv("cm");

        logger.info("cm : " + cm);

        if (StringUtils.isEmpty(cm)) {
            cm = "1";
        }

        int      cmi      = Integer.parseInt(cm);
        FloodJob floodJob = new FloodJob(jobId, 1 * cmi, 1024 * cmi);
        String   netUrl1  = System.getenv("netUrl");

        logger.info("netUrl : " + netUrl1);

        String zk = System.getenv("zk");

        netUrl = StringUtils.isEmpty(netUrl1)
                 ? netUrl
                 : netUrl1;

        List<String> zkList  = Lists.newArrayList();
        String[]     zkArray = zk.split(",");

        for (String zkHost : zkArray) {
            zkList.add(zkHost);
        }

        String       nimbusSeeds     = System.getenv("nimbusSeeds");
        List<String> nimbusSeedsList = Lists.newArrayList();
        String[]     nimbusArray     = nimbusSeeds.split(",");

        for (String nimbusHost : nimbusArray) {
            nimbusSeedsList.add(nimbusHost);
        }

        // 调用服务
        String zkHostsArrays = "[";

        for (Object zkHost : zkList) {
            zkHostsArrays = zkHostsArrays + "\\\"" + zkHost + "\\\"" + " ";
        }

        zkHostsArrays = zkHostsArrays.substring(0, zkHostsArrays.length() - 1) + "]";
        dockerArgs    = dockerArgs + " -c storm.zookeeper.servers=" + zkHostsArrays;

        String nimbusSeedsArray = "[";

        for (Object nimbus : nimbusSeedsList) {
            nimbusSeedsArray = nimbusSeedsArray + "\\\"" + nimbus + "\\\"" + ",";
        }

        nimbusSeedsArray = nimbusSeedsArray.substring(0, nimbusSeedsArray.length() - 1) + "]";
        dockerArgs       = dockerArgs + " -c nimbus.seeds=[\\\"" + nimbusSeedsArray + "\\\"]";
        floodJob.netUrl(netUrl);
        floodJob.buildDockerCMD()
                .imageName(imageName)
                .containerName(containerName)
                .hostName(containerName)
                .host(containerName, dockerIp)
                .ip(dockerIp)
                .hosts(host)
                .ports(port)
                .dockerArgs(dockerArgs);
        floodContrJobPubProxy.publishJob(floodJob, FloodJob.PRIORITY.DEFAULT_PRIORITY);
    }

    @Override
    public String getAllDockerJob() throws TException {
        return JSONObject.toJSONString(FloodContrRunningMonitor.getFloodJobRunningState());
    }

    @Override
    public String restartDocker(String jobId) throws TException {

        FloodJobRunningState floodJobRunningState =  FloodContrRunningMonitor.floodJobRunningStates.get(jobId);
        if(floodJobRunningState == null){
            return "任务已不存在";
        }else if(floodJobRunningState.getRunningState() == FloodJobRunningState.RUNNING_STATE.RESTARTING ){
            return "正在重启，无需再重启";
        }

        FloodJob floodJob = floodJobRunningState.getFloodJob().clone();

        floodJob.launch_type(FloodJob.LAUNCH_TYPE.RESTART)
                .nodeBind(floodJobRunningState.getRunIp())
                .priority(FloodJob.PRIORITY.HIGH);

        floodJobRunningState.setFloodJob(floodJob.clone());

        floodContrJobPubProxy.publishJob(floodJob,floodJobRunningState.getRunIp(),FloodJob.PRIORITY.HIGH);
        return "已重新发布 "+floodJob.getJobId();
    }


    @Override
    public String getStormNimbus() throws TException {
        List<String>               result = Lists.newArrayList();
        List<FloodJobRunningState> list   = FloodContrRunningMonitor.getFloodJobRunningState();

        if (CollectionUtils.isEmpty(list)) {
            return "";
        }

        for (FloodJobRunningState floodJobRunningState : list) {
            if (floodJobRunningState.getFloodJob().getBusinessTag().equals("nimbus")) {
                result.add(floodJobRunningState.getFloodJob().getDockerCMD().getContainerName() + ":"
                           + floodJobRunningState.getRunIp() + ":9005");
            }
        }

        return JSONObject.toJSONString(result);
    }

    @Override
    public String getStormUi() throws TException {
        List<FloodJobRunningState> list = FloodContrRunningMonitor.getFloodJobRunningState();

        if (CollectionUtils.isEmpty(list)) {
            return "";
        }

        for (FloodJobRunningState floodJobRunningState : list) {
            if (floodJobRunningState.getFloodJob().getBusinessTag().equals("ui")) {
                return floodJobRunningState.getRunIp() + ":9092";
            }
        }

        return null;
    }
}
