/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */



package com.yss.yarn;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.*;

import java.net.URL;
import java.util.*;


public class Config {
    final public static String MASTER_DEFAULTS_CONFIG    = "master_defaults.yaml";
    final public static String MASTER_CONFIG             = "master.yaml";
    final public static String MASTER_HOST               = "master.host";
    final public static String MASTER_THRIFT_PORT        = "master.thrift.port";
    final public static String MASTER_TIMEOUT_SECS       = "master.timeout.secs";
    final public static String MASTER_SIZE_MB            = "master.container.size-mb";
    final public static String MASTER_NUM_SUPERVISORS    = "master.initial-num-supervisors";
    final public static String MASTER_CONTAINER_PRIORITY = "master.container.priority";

    // # of milliseconds to wait for YARN report on Storm Master host/port
    final public static String YARN_REPORT_WAIT_MILLIS          = "yarn.report.wait.millis";
    final public static String MASTER_HEARTBEAT_INTERVAL_MILLIS = "master.heartbeat.interval.millis";

    @SuppressWarnings("rawtypes")
    static public Map readStormConfig() {
        return readStormConfig(null);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Map readStormConfig(String stormYarnConfigPath) {

        // default configurations

        // standard storm configuration

        Map ret = new HashMap();

        Map conf = findAndReadConfigFile(Config.MASTER_DEFAULTS_CONFIG,false);
        ret.putAll(conf);

        String confFile = System.getProperty("storm.conf.file");
        Map    storm_conf;

        if ((confFile == null) || confFile.equals("")) {
            storm_conf = findAndReadConfigFile("storm.yaml", false);
        } else {
            storm_conf = findAndReadConfigFile(confFile, true);
        }

        ret.putAll(storm_conf);

        // configuration file per command parameter
        if (stormYarnConfigPath == null) {
            Map master_conf = findAndReadConfigFile(Config.MASTER_CONFIG, false);

            ret.putAll(master_conf);
        } else {
            try {
                Yaml            yaml              = new Yaml();
                FileInputStream is                = new FileInputStream(stormYarnConfigPath);
                Map             storm_yarn_config = (Map) yaml.load(is);

                if (storm_yarn_config != null) {
                    ret.putAll(storm_yarn_config);
                }

                is.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // other configuration settings via CLS opts per system property: storm.options
        return ret;
    }

    public static Map<String, Object> findAndReadConfigFile(String name, boolean mustExist) {
        InputStream in = null;
        boolean confFileEmpty = false;
        try {
            in = getConfigFileInputStream(name);
            if (null != in) {
                Yaml yaml = new Yaml(new SafeConstructor());
                @SuppressWarnings("unchecked")
                Map<String, Object> ret = (Map<String, Object>) yaml.load(new InputStreamReader(in));
                if (null != ret) {
                    return new HashMap<>(ret);
                } else {
                    confFileEmpty = true;
                }
            }

            if (mustExist) {
                if(confFileEmpty)
                    throw new RuntimeException("Config file " + name + " doesn't have any valid storm configs");
                else
                    throw new RuntimeException("Could not find config file on classpath " + name);
            } else {
                return new HashMap<>();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static InputStream getConfigFileInputStream(String configFilePath)
            throws IOException {
        if (null == configFilePath) {
            throw new IOException(
                    "Could not find config file, name not specified");
        }

        HashSet<URL> resources = new HashSet<URL>(findResources(configFilePath));
        if (resources.isEmpty()) {
            File configFile = new File(configFilePath);
            if (configFile.exists()) {
                return new FileInputStream(configFile);
            }
        } else if (resources.size() > 1) {
            throw new IOException(
                    "Found multiple " + configFilePath
                            + " resources. You're probably bundling the Storm jars with your topology jar. "
                            + resources);
        } else {
//            LOG.debug("Using "+configFilePath+" from resources");
            URL resource = resources.iterator().next();
            return resource.openStream();
        }
        return null;
    }

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
