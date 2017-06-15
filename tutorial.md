本实例主要是使用Hadoop MapReduce和HBase分析Web日志，统计每天的PV和UV。

###步骤1： 输入数据准备
Web访问日志具备如下格式：

    $remote_addr - [$time_local] "$request" $status $body_bytes_sent "$http_referer"  $http_cookie" $remote_user "$http_user_agent" $request_time  $host $msec

例如：

    10.81.78.220 - [04/Oct/2015:21:31:22 +0800] "GET /u2bmp.html?dm=37no6.com/003&ac=1510042131161237772&v=y88j6-1.0&rnd=1510042131161237772&ext_y88j6_tid=003&ext_y88j6_uid=1510042131161237772 HTTP/1.1" 200 54 "-" "-" 9CA13069CB4D7B836DC0B8F8FD06F8AF "ImgoTV-iphone/4.5.3.150815 CFNetwork/672.1.13 Darwin/14.0.0" 0.004 test.com.org 1443965482.737

用户可以使用`bos://bmr-public-data/logs/accesslog-1k.log`中提供的实例数据或者按照如上格式构造自己的输入数据并上传到BOS(对象存储，具体可参见BOS使用说明)。

###步骤2：上传客户端程序的jar包至BOS
具体程序见`maven`项目

编译打包后将jar包上传到自己的BOS空间中。下面用bos://${USER_BUCKET}/表示用户自己的空间。

###步骤3：
创建一个Hadoop镜像的BMR集群。

###步骤4：
提取Web访问日志内容到HBase Table。
从console页面进去到对应集群的作业列表页面，然后点击“增加作业”，可以按照如下方式填写参数：

>作业类型：Custom Jar

>名称：HBaseETL 

>bos输入地址： bos://${USER_BUCKET}/bmr-hbase-samples-1.0-SNAPSHOT.jar

>失败后操作：继续

>MainClass：com.baidubce.bmr.hbase.samples.logextract.AccessLogExtract

>应用程序参数：-D mapreduce.job.maps=6 -D mapreduce.job.reduces=2 bos://bmr-public-data/logs/accesslog-1k.log AccessTable

其中“应用程序参数”中最后一个参数"AccessTable"，是HBase Table的名称。

###步骤5：
统计每天的PV。
同样要在console增加作业，填写参数如下：
>作业类型：Custom Jar

>名称：PV 

>bos输入地址： bos://${USER_BUCKET}/bmr-hbase-samples-1.0-SNAPSHOT.jar

>失败后操作：继续

>MainClass：com.baidubce.bmr.hbase.samples.pv.PageView

>应用程序参数：-D mapreduce.job.maps=6 -D mapreduce.job.reduces=2 AccessTable bos://${USER_BUCKET}/pv

bos://${USER_BUCKET}/pv/下的最终reduce结果示例：

    03/Oct/2015	139
    05/Oct/2015	372
    04/Oct/2015	375
    06/Oct/2015	114
    
###步骤6：
统计每天的UV。
同样要在console增加作业，填写参数如下：
>作业类型：Custom Jar

>名称：PV 

>bos输入地址： bos://${USER_BUCKET}/bmr-hbase-samples-1.0-SNAPSHOT.jar

>失败后操作：继续

>MainClass：com.baidubce.bmr.hbase.samples.uv.UniqueVisitor

>应用程序参数：-D mapreduce.job.maps=6 -D mapreduce.job.reduces=2 AccessTable bos://${USER_BUCKET}/uv

bos://${USER_BUCKET}/uv/下的最终reduce结果示例：

    03/Oct/2015	111
    05/Oct/2015	212
    04/Oct/2015	247
    06/Oct/2015	97

