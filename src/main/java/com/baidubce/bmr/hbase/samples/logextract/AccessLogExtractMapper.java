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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapper class for AccessLogExtract.
 * 
 */
public class AccessLogExtractMapper 
    extends Mapper<Object, Text, ImmutableBytesWritable, Put> {

  private static final String SEPARATOR = "\\s{1}";
  private static final String REMOTE_ADDR_PATTERN = "(\\S+)";
  private static final String TIME_LOCAL_PATTERN = "\\[(.*?)\\]";
  private static final String REQUEST_PATTERN = "(.*?)";
  private static final String STATUS_PATTERN = "(\\d{3})";
  private static final String BODY_BYTES_SENT_PATTERN = "(\\S+)";
  private static final String HTTP_REFERER_PATTERN = "(.*?)";
  private static final String HTTP_COOKIE_PATTERN = "(.*?)";
  private static final String REMOTE_USER_PATTERN = "(.*?)";
  private static final String HTTP_USER_AGENT_PATTERN = "(.*?)";
  private static final String REQUEST_TIME_PATTERN = "(\\S+)";
  private static final String HOST_PATTERN = "(\\S+)";
  private static final String MSEC_PATTERN = "(\\S+)";

  private static final String LINE_REGEX = REMOTE_ADDR_PATTERN + SEPARATOR + "-" 
      + SEPARATOR + TIME_LOCAL_PATTERN + SEPARATOR + REQUEST_PATTERN 
      + SEPARATOR + STATUS_PATTERN + SEPARATOR + BODY_BYTES_SENT_PATTERN 
      + SEPARATOR + HTTP_REFERER_PATTERN + SEPARATOR + HTTP_COOKIE_PATTERN 
      + SEPARATOR + REMOTE_USER_PATTERN + SEPARATOR + HTTP_USER_AGENT_PATTERN
      + SEPARATOR + REQUEST_TIME_PATTERN + SEPARATOR + HOST_PATTERN 
      + SEPARATOR + MSEC_PATTERN;

  @Override
  protected void map(Object key, Text value, Context context) throws IOException,
      InterruptedException {
    String line = value.toString();
    Pattern pattern = Pattern.compile(LINE_REGEX);
    Matcher matcher = pattern.matcher(line);

    if (!matcher.matches()) {
      return;
    }
    if (matcher.groupCount() != 12) {
      return;
    }

    byte[] rowKey = Bytes.toBytes(UUID.randomUUID().toString());
    Put put = new Put(rowKey);
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_REMOTE_ADDR, 
        Bytes.toBytes(matcher.group(1)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_TIME_LOCAL, 
        Bytes.toBytes(matcher.group(2)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_REQUEST, 
        Bytes.toBytes(matcher.group(3)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_STATUS, 
        Bytes.toBytes(matcher.group(4)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_BODY_BYTES_SENT,
        Bytes.toBytes(matcher.group(5)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_HTTP_REFERER, 
        Bytes.toBytes(matcher.group(6)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_HTTP_COOKIE, 
        Bytes.toBytes(matcher.group(7)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_REMOTE_USER, 
        Bytes.toBytes(matcher.group(8)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_HTTP_USER_AGENT,
        Bytes.toBytes(matcher.group(9)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_REQUEST_TIME,
        Bytes.toBytes(matcher.group(10)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_HOST, 
        Bytes.toBytes(matcher.group(11)));
    put.add(TableInfo.COLUMN_FAMILY, TableInfo.COLUMN_MSEC, 
        Bytes.toBytes(matcher.group(12)));

    context.write(new ImmutableBytesWritable(rowKey), put);
  }
}
