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

package com.baidubce.bmr.hbase.samples.uv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Reducer class for UniqueVisitor.
 * 
 */
public class UniqueVisitorReducer extends Reducer<Text, Text, Text, IntWritable> {

  private IntWritable count = new IntWritable();

  @Override
  protected void reduce(Text date, Iterable<Text> ipList, Context context) 
      throws IOException, InterruptedException {
    HashSet<String> ipSet = new HashSet<String>();
    for (Text ip : ipList) {
      ipSet.add(ip.toString());
    }

    count.set(ipSet.size());
    context.write(date, count);
  }

}
