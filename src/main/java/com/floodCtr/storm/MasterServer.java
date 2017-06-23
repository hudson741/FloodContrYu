package com.floodCtr.storm;

import com.floodCtr.YarnClient;
import com.floodCtr.job.FloodJob;
import com.floodCtr.publish.FloodContrJobPubProxy;
import com.floodCtr.publish.PRIORITY;
import com.floodCtr.rpc.FloodContrMaster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.UUID;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class MasterServer {

    public static void main(String[] args) throws IOException, YarnException {

        /**
         * 个性化thrift接口实现，每个使用者自定义实现
         */
        StormService.Iface handler          = new StormServiceImpl();

        /**
         * 如果没有特殊hadoop使用配置，固定这么写就行
         */
        YarnClient yarnClient = new YarnClient(new YarnConfiguration());

        /**
         * 用于做初始化提交，也可以不写，当MasterServer启动后，通过thrift交互提交。
         */
        final FloodContrJobPubProxy floodContrJobPubProxy= new FloodContrJobPubProxy(yarnClient);


        FloodContrMaster floodContrMaster = new FloodContrMaster(yarnClient,new StormService.Processor<>(handler), 9001) {
            /**
             * 初始化执行，用于server启动后，默认执行的方法
             */
            @Override
            public void initExecute() {

                /**
                 * 构建storm docker容器，发布floodJob
                 */
                String   jobId         = UUID.randomUUID().toString();
                FloodJob floodJob      = new FloodJob(jobId, 1, 1024);
                String   imageName     = "storm";
                String   containerName = "ui-3";
                String   ip            = "192.168.10.6";
                String   dockerArgs    =
                        "storm ui -c storm.zookeeper.servers=[\\\"10.186.58.13\\\"] -c nimbus.seeds=[\\\"192.168.10.3\\\",\\\"192.168.10.4\\\"] -c nimbus.thrift.port=9005 -c ui.port=9002";

                floodJob.buildDockerCMD()
                        .imageName(imageName)
                        .containerName(containerName)
                        .hostName(containerName)
                        .host(containerName, ip)
                        .ip(ip)
                        .dockerArgs(dockerArgs);

                floodContrJobPubProxy.publishJob(floodJob, PRIORITY.DEFAULT_PRIORITY);
            }
        };

        floodContrMaster.start();
    }

    /**
     * 这里为自定义thrift接口，每个客户端做自我实现，用于与运行中的masterServer做交互
     */
    public static class StormServiceImpl implements StormService.Iface {
        @Override
        public String addSupervisor(String num) throws TException {
            String   jobId         = UUID.randomUUID().toString();
            FloodJob floodJob      = new FloodJob(jobId, 1, 1024);
            String   imageName     = "storm";
            String   containerName = "ui-3";
            String   ip            = "192.168.10.6";
            String   dockerArgs    =
                    "storm ui -c storm.zookeeper.servers=[\"10.186.58.13\"] -c nimbus.seeds=[\"192.168.10.3\",\"192.168.10.4\"] -c nimbus.thrift.port=9005 -c ui.port=9002";

            floodJob.buildDockerCMD()
                    .imageName(imageName)
                    .containerName(containerName)
                    .hostName(containerName)
                    .host(containerName, ip)
                    .ip(ip)
                    .dockerArgs(dockerArgs);

            return null;
        }
    }

}
