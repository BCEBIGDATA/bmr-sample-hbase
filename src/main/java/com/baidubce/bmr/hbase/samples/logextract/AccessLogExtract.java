/*
 * Copyright (C) 2015 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidubce.bmr.hbase.samples.logextract;

import com.baidubce.bmr.hbase.samples.TableInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Extract, Transform and Load HTTP access log into HBase.
 * 
 */
public class AccessLogExtract {

  private static void createTable(final String tableName, HBaseAdmin admin) 
      throws IOException {
    if (admin.tableExists(tableName)) {
      return;
    }

    HColumnDescriptor family = new HColumnDescriptor(TableInfo.COLUMN_FAMILY);
    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
    table.addFamily(family);

    admin.createTable(table);
  }

  private static void createTable(final String tableName, final Configuration conf) 
      throws IOException {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(conf);
      createTable(tableName, admin);
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  private static Job configureJob(final Configuration conf, final String[] args)
      throws IOException {
    final Path inputPath = new Path(args[0]);
    final String tableName = args[1];

    createTable(tableName, conf);

    Job job = Job.getInstance(conf, "AccessLogExtract");
    job.setJarByClass(AccessLogExtract.class);

    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    job.setMapperClass(AccessLogExtractMapper.class);

    job.setNumReduceTasks(0);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);

    return job;
  }

  /**
   * Main entrance for AccessLogExtract.
   * 
   */
  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();

    String[] otherArgs = null;
    try {
      otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(2);
    }

    if (otherArgs.length != 2) {
      System.err.println("Usage: AccessLogExtract <source path> <target table name>");
      System.exit(2);
    }

    try {
      Job job = configureJob(conf, otherArgs);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
