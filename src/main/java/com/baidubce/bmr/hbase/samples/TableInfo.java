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

package com.baidubce.bmr.hbase.samples;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Table info of HBase.
 * 
 */
public class TableInfo {
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("AccessCF");

  public static final byte[] COLUMN_REMOTE_ADDR = Bytes.toBytes("remote_addr");
  public static final byte[] COLUMN_TIME_LOCAL = Bytes.toBytes("time_local");
  public static final byte[] COLUMN_REQUEST = Bytes.toBytes("request");
  public static final byte[] COLUMN_STATUS = Bytes.toBytes("status");
  public static final byte[] COLUMN_BODY_BYTES_SENT = Bytes.toBytes("body_bytes_sent");
  public static final byte[] COLUMN_HTTP_REFERER = Bytes.toBytes("http_referer");
  public static final byte[] COLUMN_HTTP_COOKIE = Bytes.toBytes("http_cookie");
  public static final byte[] COLUMN_REMOTE_USER = Bytes.toBytes("remote_user");
  public static final byte[] COLUMN_HTTP_USER_AGENT = Bytes.toBytes("http_user_agent");
  public static final byte[] COLUMN_REQUEST_TIME = Bytes.toBytes("request_time");
  public static final byte[] COLUMN_HOST = Bytes.toBytes("host");
  public static final byte[] COLUMN_MSEC = Bytes.toBytes("msec");
}
