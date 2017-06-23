package com.floodCtr.storm;

import java.io.IOException;

import org.apache.hadoop.yarn.exceptions.YarnException;

import com.floodCtr.YarnClient;
import com.floodCtr.generate.StormThriftService;
import com.floodCtr.publish.FloodContrJobPubProxy;
import com.floodCtr.rpc.FloodContrMaster;

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
        final FloodContrJobPubProxy floodContrJobPubProxy = new FloodContrJobPubProxy(YarnClient.getInstance());
        FloodContrMaster      floodContrMaster      =
            new FloodContrMaster(new StormThriftService.Processor<>(new StormThriftServiceImpl(floodContrJobPubProxy)),
                                 9000) {
            @Override
            public void initExecute() {

            }
        };

        floodContrMaster.start();
    }
}
