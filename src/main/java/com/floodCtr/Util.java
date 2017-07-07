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



package com.floodCtr;

import java.io.*;

import java.net.URL;

import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;


public class Util {
    static Logger              logger                 = Logger.getLogger(Util.class);

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL>        ret       = new ArrayList<URL>();

            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }

            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static LocalResource newYarnAppResource(FileSystem fs, Path path, LocalResourceType type,
                                                   LocalResourceVisibility vis)
            throws IOException {
        Path          qualified = fs.makeQualified(path);
        FileStatus    status    = fs.getFileStatus(qualified);
        LocalResource resource  = Records.newRecord(LocalResource.class);

        resource.setType(type);
        resource.setVisibility(vis);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified));
        resource.setTimestamp(status.getModificationTime());
        resource.setSize(status.getLen());

        return resource;
    }

    public  static InputStream getConfigFileInputStream(String configFilePath) throws IOException {
        if (null == configFilePath) {
            throw new IOException("Could not find config file, name not specified");
        }

        HashSet<URL> resources = new HashSet<URL>(findResources(configFilePath));

        if (resources.isEmpty()) {
            File configFile = new File(configFilePath);

            if (configFile.exists()) {
                return new FileInputStream(configFile);
            }
        } else if (resources.size() > 1) {
            throw new IOException("Found multiple " + configFilePath
                                  + " resources. You're probably bundling the Storm jars with your topology jar. "
                                  + resources);
        } else {

//          LOG.debug("Using "+configFilePath+" from resources");
            URL resource = resources.iterator().next();

            return resource.openStream();
        }

        return null;
    }

}
