package com.yss.yarn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.net.URI;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;

import com.google.common.collect.Lists;

import com.yss.yarn.resource.IPManager;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/2
 */
public class TestMain {
    public static List<String> buildStormOnDockerCommond(StormDockerService.PROCESS process, String containerId,
                                                  List<Object> zkHosts, List<String> nimbusSeeds, int nimbusThriftPort,
                                                  int memory, int cpu, int uiPort, String ip) {
        Vector<CharSequence> vargs = new Vector<CharSequence>(100);

        vargs.add("sh runStormOnDocker.sh ");

        // 1 containerName
        vargs.add("'" + process.getCode() + "-" + containerId);

        // 4 nimbus port
        vargs.add(String.valueOf(nimbusThriftPort));

        // 5  memery
        vargs.add(memory + "MB");

        // 6  cpu
        vargs.add(String.valueOf(cpu));

        // 7  uiport
        vargs.add(String.valueOf(uiPort));

        // 8  process
        vargs.add(process.getCode());

        // 9 ip
        vargs.add(ip + "'");

        //  zk
        String zkHostsArrays = "'";

        for (Object zkHost : zkHosts) {
            zkHostsArrays += zkHost + " ";
        }

        zkHostsArrays = zkHostsArrays.substring(0, zkHostsArrays.length() - 1) + "'";
        vargs.add(zkHostsArrays);

        //  nimbus.seeds
        String nmSeedsArrays = "'";

        for (String nimbus : nimbusSeeds) {
            nmSeedsArrays += nimbus + " ";
        }

        nmSeedsArrays = nmSeedsArrays.substring(0, nmSeedsArrays.length() - 1) + "'";
        vargs.add(nmSeedsArrays);

        vargs.add("1> /opt/stdout");
        vargs.add("2> /opt/stderr");

        StringBuilder command = new StringBuilder();

        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        List<String> commands = new ArrayList<String>();

        commands.add(command.toString());

        return commands;
    }
    public static void main(String[] args) throws Exception {
        try {
            String dsf = "hdfs://123.207.8.134:9000/docker/work/shell/runStormOnDocker.sh";
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(dsf),conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(dsf));

            byte[] ioBuffer = new byte[10240];
            int readLen = hdfsInStream.read(ioBuffer);
            while(readLen!=-1)
            {
                System.out.write(ioBuffer, 0, readLen);
                readLen = hdfsInStream.read(ioBuffer);
            }
            hdfsInStream.close();
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static byte[] readStream(InputStream inStream) throws Exception {
        ByteArrayOutputStream outSteam = new ByteArrayOutputStream();
        byte[]                buffer   = new byte[1024];
        int                   len      = -1;

        while ((len = inStream.read(buffer)) != -1) {
            outSteam.write(buffer, 0, len);
        }

        outSteam.close();
        inStream.close();

        return outSteam.toByteArray();
    }
}
