/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.engine.mr;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Writable;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtil {
    private static final ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();
    private static final Logger logger = LoggerFactory.getLogger(HadoopUtil.class);
    public static void setCurrentConfiguration(Configuration conf) {
        logger.info("***************override yarn config*****************");
        conf.set("fs.azure.account.key.adsscp.blob.core.windows.net","1g19kHbMCLgWB/XFUKrHFMFgIW6iEd6dtQbhjlcmjStScngrjkG4RRGNXdBR6Jnu7VoFF+p9LaoMUaQuPHu4EQ==");
        conf.unset("fs.azure.account.keyprovider.adsscp.blob.core.windows.net");
        conf.unset("fs.azure.shellkeyprovider.script");
        hadoopConfig.set(conf);
    }

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            Configuration conf = healSickConfig(new Configuration());
            return conf;
        }
        return hadoopConfig.get();
    }

    private static Configuration healSickConfig(Configuration conf) {
        // why we have this hard code?
        conf.set(DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY, "8");
        conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, KylinConfig.getInstanceFromEnv().getDefaultFs());
        conf.set(DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME,"true");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,"0.0.0.0");
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,"0.0.0.0");
        conf.set("fs.azure.account.key.adsscp.blob.core.windows.net","1g19kHbMCLgWB/XFUKrHFMFgIW6iEd6dtQbhjlcmjStScngrjkG4RRGNXdBR6Jnu7VoFF+p9LaoMUaQuPHu4EQ==");
        conf.unset("fs.azure.account.keyprovider.adsscp.blob.core.windows.net");
        conf.unset("fs.azure.shellkeyprovider.script");
        conf.set("dfs.namenode.rpc-address.mycluster.nn1","hn0-kylin.kmt3ffv3emku1j0t2j2lc1uoqh.dx.internal.cloudapp.net:8020");
        conf.set("dfs.namenode.rpc-address.mycluster.nn2","hn1-kylin.kmt3ffv3emku1j0t2j2lc1uoqh.dx.internal.cloudapp.net:8020");

        logger.info("***************setting hdfs default fs*****************");
        logger.info("***************setting hdfs default fs*****************");
        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        if (StringUtils.isBlank(conf.get("hbase.fs.tmp.dir"))) {
            conf.set("hbase.fs.tmp.dir", "/tmp");
        }
        return conf;
    }

    public static FileSystem getFileSystem(String path) throws IOException {
        return FileSystem.get(makeURI(path), getCurrentConfiguration());
    }

    public static URI makeURI(String filePath) {
        try {
            return new URI(fixWindowsPath(filePath));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from URI: " + filePath, e);
        }
    }

    public static String fixWindowsPath(String path) {
        // fix windows path
        if (path.startsWith("file://") && !path.startsWith("file:///") && path.contains(":\\")) {
            path = path.replace("file://", "file:///");
        }
        if (path.startsWith("file:///")) {
            path = path.replace('\\', '/');
        }
        return path;
    }

    /**
     * @param table the identifier of hive table, in format <db_name>.<table_name>
     * @return a string array with 2 elements: {"db_name", "table_name"}
     */
    public static String[] parseHiveTableName(String table) {
        int cut = table.indexOf('.');
        String database = cut >= 0 ? table.substring(0, cut).trim() : "DEFAULT";
        String tableName = cut >= 0 ? table.substring(cut + 1).trim() : table.trim();

        return new String[] { database, tableName };
    }

    public static void deletePath(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public static byte[] toBytes(Writable writable) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);
            writable.write(out);
            out.close();
            bout.close();
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
