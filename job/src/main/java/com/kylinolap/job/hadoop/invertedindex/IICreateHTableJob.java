/*
 * Copyright 2013-2014 eBay Software Foundation
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
 * limitations under the License.
 */
package com.kylinolap.job.hadoop.invertedindex;

import com.kylinolap.invertedindex.IIInstance;
import com.kylinolap.invertedindex.IIManager;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.invertedindex.model.IIKeyValueCodec;
import com.kylinolap.job.tools.DeployCoprocessorCLI;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.util.ToolRunner;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.common.util.HadoopUtil;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author George Song (ysong1)
 */
public class IICreateHTableJob extends AbstractHadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_II_NAME);
            options.addOption(OPTION_HTABLE_NAME);
            parseOptions(options, args);

            String tableName = getOptionValue(OPTION_HTABLE_NAME);
            String iiName = getOptionValue(OPTION_II_NAME);

            IIInstance ii = IIManager.getInstance(KylinConfig.getInstanceFromEnv()).getII(iiName);
            int sharding = ii.getDescriptor().getSharding();

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor cf = new HColumnDescriptor(IIDesc.HBASE_FAMILY);
            cf.setMaxVersions(1);
            //cf.setCompressionType(Algorithm.LZO);
            cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            tableDesc.addFamily(cf);

            Configuration conf = HBaseConfiguration.create(getConf());
            if (User.isHBaseSecurityEnabled(conf)) {
                // add coprocessor for bulk load
                tableDesc.addCoprocessor("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
            }

            DeployCoprocessorCLI.deployCoprocessor(tableDesc);

            // drop the table first
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // create table
            byte[][] splitKeys = getSplits(sharding);
            if (splitKeys.length == 0)
                splitKeys = null;
            admin.createTable(tableDesc, splitKeys);
            if (splitKeys != null) {
                for (int i = 0; i < splitKeys.length; i++) {
                    System.out.println("split key " + i + ": " + BytesUtil.toHex(splitKeys[i]));
                }
            }
            System.out.println("create hbase table " + tableName + " done.");
            admin.close();

            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    //one region for one shard
    private byte[][] getSplits(int shard) {
        byte[][] result = new byte[shard - 1][];
        for (int i = 1; i < shard; ++i) {
            byte[] split = new byte[IIKeyValueCodec.SHARD_LEN];
            BytesUtil.writeUnsigned(i, split, 0, IIKeyValueCodec.SHARD_LEN);
            result[i - 1] = split;
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        IICreateHTableJob job = new IICreateHTableJob();
        job.setConf(HadoopUtil.newHBaseConfiguration(KylinConfig.getInstanceFromEnv().getStorageUrl()));
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
