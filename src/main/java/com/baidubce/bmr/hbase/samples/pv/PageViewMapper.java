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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Mapper class for PageView.
 * 
 */
public class PageViewMapper extends TableMapper<Text, IntWritable> {

  private Text date = new Text();
  private IntWritable one = new IntWritable(1);

  @Override
  protected void map(ImmutableBytesWritable rowKey, Result result, Context context)
      throws IOException, InterruptedException {
    byte[] timeLocal = result.getValue(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_TIME_LOCAL);
    String dateStr = (new String(timeLocal)).split(":")[0];
    date.set(dateStr);
    context.write(date, one);
  }
}
