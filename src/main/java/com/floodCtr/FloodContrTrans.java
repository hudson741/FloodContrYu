package com.floodCtr;

import java.io.IOException;
import java.io.InputStream;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

import org.mortbay.util.StringUtil;

import com.floodCtr.job.FloodJob;

import com.google.common.collect.Lists;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class FloodContrTrans {
    private static String buildCPU(int cpu) {
        return "--cpus " + cpu;
    }

    private static String buildDockerContainerName(String containerName) {
        return StringUtils.isEmpty(containerName)
               ? ""
               : "--name " + containerName;
    }

    private static String buildDockerHostName(String hostName) {
        return StringUtils.isEmpty(hostName)
               ? ""
               : "--hostname " + hostName;
    }

    private static String buildDockerImageCMD(String image) {
        return image;
    }

    private static String buildDockerRunCMD(FloodJob.LAUNCH_TYPE launchType) {
        if (launchType == FloodJob.LAUNCH_TYPE.NEW) {
            return "docker run -i --net=bridge --privileged=true";
        } else {
            return "docker start -i";
        }
    }

    private static String buildHostP(Map<String, String> hostMap) {
        if (MapUtils.isEmpty(hostMap)) {
            return "";
        }

        List<String> addhosts = Lists.newArrayList();

        for (String domainName : hostMap.keySet()) {
            String ip = hostMap.get(domainName);

            addhosts.add("--add-host " + domainName + ":" + ip);
        }

        StringBuilder command = new StringBuilder();

        for (CharSequence str : addhosts) {
            command.append(str).append(" ");
        }

        return command.toString();
    }

    private static String buildMemory(int memory) {
        return "--memory " + memory + "MB";
    }

    private static String buildPortM(Map<String, String> portMap) {
        if (MapUtils.isEmpty(portMap)) {
            return "";
        }

        List<String> addPort = Lists.newArrayList();

        for (String dockerPort : portMap.keySet()) {
            String hostPort = portMap.get(dockerPort);

            addPort.add("-p " + dockerPort + ":" + hostPort);
        }

        StringBuilder command = new StringBuilder();

        for (CharSequence str : addPort) {
            command.append(str).append(" ");
        }

        return command.toString();
    }

    private static String buildVolume(Map<String, String> map) {
        if (MapUtils.isEmpty(map)) {
            return "";
        }

        List<String> addVolume = Lists.newArrayList();

        for (String localDir : map.keySet()) {
            String dockerDir = map.get(localDir);

            addVolume.add("-v " + localDir + ":" + dockerDir);
        }

        StringBuilder command = new StringBuilder();

        for (CharSequence str : addVolume) {
            command.append(str).append(" ");
        }

        return command.toString();
    }

    public static List<String> floodJobToYarnCmd(FloodJob floodJob) throws IOException {
        String       dockerCommond = transJob2Cmd(floodJob);
        InputStream  inputStream   = Util.getConfigFileInputStream("net.shell");
        LineIterator lineIterator  = IOUtils.lineIterator(inputStream, "UTF-8");
        List<String> result        = Lists.newArrayList();

        while (lineIterator.hasNext()) {
            String line = lineIterator.nextLine();

            line = StringUtils.replace(line, "$containerName", floodJob.getDockerCMD().getContainerName());
            line = StringUtil.replace(line, "$ip", floodJob.getDockerCMD().getIp());
            line = StringUtil.replace(line, "$hostName", floodJob.getDockerCMD().getHostName());
            line = StringUtil.replace(line, "$netUrl", System.getenv("netUrl"));
            result.add(line);
        }

        result.add("commond=\"" + dockerCommond + "\"");
        result.add("$commond");

        return result;
    }

    private static String transJob2Cmd(FloodJob floodJob) {
        FloodJob.DockerCMD   dockerCMD = floodJob.getDockerCMD();
        Vector<CharSequence> args      = new Vector<CharSequence>(300);

        args.add(buildDockerRunCMD(floodJob.getLaunch_type()));

        if (floodJob.getLaunch_type() == FloodJob.LAUNCH_TYPE.RESTART) {
            args.add(dockerCMD.getContainerName());
        } else {
            Map<String, String> hostMap = dockerCMD.getHost();
            Map<String, String> portM   = dockerCMD.getPort();
            Map<String, String> volume  = dockerCMD.getVolume();

            args.add(buildDockerContainerName(dockerCMD.getContainerName()));
            args.add(buildDockerHostName(dockerCMD.getHostName()));
            args.add(buildHostP(hostMap));
            args.add(buildVolume(volume));
            args.add(buildPortM(portM));
            args.add(buildCPU(floodJob.getCpu()));
            args.add(buildMemory(floodJob.getMemory()));
            args.add(buildDockerImageCMD(dockerCMD.getImageName()));
            args.add(dockerCMD.getDockerArgs());
        }

        StringBuilder command = new StringBuilder();

        for (CharSequence str : args) {
            command.append(str).append(" ");
        }

        return command.toString();
    }
}
