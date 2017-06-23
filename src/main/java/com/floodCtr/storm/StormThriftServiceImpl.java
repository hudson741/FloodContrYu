package com.floodCtr.storm;
import com.floodCtr.generate.StormThriftService;
import com.floodCtr.job.FloodJob;
import com.floodCtr.publish.FloodContrJobPubProxy;
import com.floodCtr.publish.PRIORITY;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.UUID;

/**
 * 这里为自定义thrift接口，每个客户端做自我实现，用于与运行中的masterServer做交互
 */
public  class StormThriftServiceImpl implements StormThriftService.Iface {

    public StormThriftServiceImpl(FloodContrJobPubProxy floodContrJobPubProxy){
        this.floodContrJobPubProxy = floodContrJobPubProxy;
    }

    private FloodContrJobPubProxy floodContrJobPubProxy;

    @Override
    public void addSupervisor(String containerName, String dockerIp,String dockerArgs, Map<String, String> host) throws TException {

        /**
         * 用于做初始化提交，也可以不写，当MasterServer启动后，通过thrift交互提交。
         */

        String   jobId         = UUID.randomUUID().toString();
        FloodJob floodJob      = new FloodJob(jobId, 1, 1024);
        String   imageName     = "storm";
//            String   containerName = "ui-3";
        String   ip            = "192.168.10.5";
//            String   dockerArgs    =
//                    "storm ui -c storm.zookeeper.servers=[\"10.186.58.13\"] -c nimbus.seeds=[\"192.168.10.3\",\"192.168.10.4\"] -c nimbus.thrift.port=9005 -c ui.port=9002";
        floodJob.buildDockerCMD()
                .imageName(imageName)
                .containerName(containerName)
                .hostName(containerName)
                .host(containerName, ip)
                .ip(ip)
                .dockerArgs(dockerArgs);
        floodContrJobPubProxy.publishJob(floodJob, PRIORITY.DEFAULT_PRIORITY);

    }


    @Override
    public void addNimbus(String containerName, String dockerIp, String dockerArgs, Map<String, String> host) throws TException {

    }

    @Override
    public void addUi(String containerName, String dockerIp, String dockerArgs, Map<String, String> host) throws TException {

    }

    public FloodContrJobPubProxy getFloodContrJobPubProxy() {
        return floodContrJobPubProxy;
    }

    public void setFloodContrJobPubProxy(FloodContrJobPubProxy floodContrJobPubProxy) {
        this.floodContrJobPubProxy = floodContrJobPubProxy;
    }
}