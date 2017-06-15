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

package com.baidubce.bmr.hbase.samples.pv;

import com.baidubce.bmr.hbase.samples.TableInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Get page views of every day.
 * 
 */
public class PageView {

  private static Job configureJob(final Configuration conf, final String[] args) 
      throws IOException {
    final String tableName = args[0];
    final Path outputPath = new Path(args[1]);

    Job job = Job.getInstance(conf, "PageView");
    job.setJarByClass(PageView.class);

    Scan scan = new Scan();
    scan.setBatch(0);
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addColumn(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_TIME_LOCAL);
    TableMapReduceUtil.initTableMapperJob(tableName, scan, PageViewMapper.class, 
        Text.class, IntWritable.class, job);

    job.setReducerClass(PageViewReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job;
  }

  /**
   * Main entrance for PageView.
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
      System.err.println("Usage: PageView <source table name> <target path>");
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
