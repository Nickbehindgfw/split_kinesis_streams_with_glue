使用 AWS Glue 从 Kinesis 数据流中分离出不同的数据库表格
================================================

## RDBMS 与数据湖

关系型数据库是数据分析过程中非常普遍的一个数据源。一般我们会通过 ETL 过程，将数据库中的数据采集并经过清洗、集合、转换，再由后端分析工具产生我们需要的分析结果。

在现代数据仓库架构中，我们推荐基于 Amazon Simple Storage(S3)  的数据湖体系结构，AWS Database Migration Service(DMS) 能帮助我们完成关系型数据库到 S3 的全量和增量数据采集。其操作过程非常简单：
1. 准备 DMS 环境，包括创建 VPC、VPC 子网、IAM 角色和 EC2 安全组，创建 DMS 子网组；
2. 创建 DMS 复制实例，因为 DMS 需要缓存从任务开始时起的数据库变更，所以预留好内存和硬盘应对需要。生产环境下，建议启用 Multi-AZ 保证 DMS 的高可用；
3. 建立指向源数据库和 S3 的终端节点，确保复制实例可以成功连接终端节点；
4. 创建并启动迁移任务，数据库记录就会源源不断的进入S3。

DMS 会按每个表一个目录的方式，把数据库记录存储为 CSV 或 Parquet 格式的 S3 对象。AWS 的 ETL 工具 AWS Glue 可以通过爬虫程序爬取表结构，存储在统一的元数据存储——数据目录中，供各种分析工具调用，比如说，使用 Amazon Athena 或者 Amazon Redshift Spectrum 进行即席查询。

## 消费数据库日志流

有些时候，我们希望更加迅速的访问到数据库的变更内容，这时如果通过 S3 中转，增加了处理时延，不符合我们的性能需求。这个时候，我们会引入流处理框架。

Amazon Kinesis Data Streams 是在 Amazon 内部和外部都得到广泛使用的流式存储引擎。我们通过 Amazon Kinesis Data Streams，把数据表通过 Kinesis 转化为数据流。

不过这种情况下，如果我们想复用这个数据流，进行批式数据处理，会遇到一些问题：当我们通过 Amazon Kinesis Firehose 把数据投递到 S3 后，我们会发现整个流的数据被放置在同一个文件夹下，而且数据是JSON格式，每条记录中包含metadata和data两个一级元素。AWS Glue 的结构爬取程序对记录结构进行解析后，会仅识别为一张只有两个字段的大表。

如果让每个表格使用独立的数据流，可以解决上述问题，但增加了管理难度；如果另起一个 DMS 进程，则会增加源库负担。是否有更其它方法呢？其实我们可以借助 Glue 对 PySpark 语法的扩展，来灵活处理此问题。

具体来讲，就是使用 filter 这个 Transform 方法，基于 metadata 中的 schema name + table name 对记录进行过滤，把不同的表格内容分离出来。

接下来，我们将通过一个 Demo 来演示具体操作，假设您已经：
1. 启用了 AWS 东京区域（ap-northeast-1，本次实验假定在东京区域）；
2. 安装并正确设置了 [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)；
3. 创建了一台 MySQL 5.7 版本的 Amazon RDS 服务器，创建 RDS 实例前先创建一个参数组，并按 [DMS 对 RDS for MySQL 源的要求](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MySQL.html#CHAP_Source.MySQL.AmazonManaged)修改参数组和实例其它设置。示例数据库可以使用 [DMS 示例数据库](https://github.com/aws-samples/aws-database-migration-samples)，在运行 install-rds.sql 建库之前，需要在 aws-database-migration-samples/mysql/sampledb/v1/ 目录下运行以下命令进行一些修正：
```
sed -i '1625d;s/Insert into/Insert ignore into/g' name_data.sql;
```
另外需要在 MySQL 中给 dms_user 读取 Binlog 的权限：
```
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO dms_user;
```

整体架构如图所示：



![image8](/Users/haofh/split_kinesis_streams_with_glue/image/image8.png)





## 1. 新建 Kinesis Data Streams 数据流和 Firehose 投递流

Kinesis Data Streams 的创建非常简单，提供 stream 名称和 shard 数量即可，以下是 CLI 命令示例：
```
aws kinesis create-stream \
  --stream-name "dms_sample" \
  --shard-count 2 \
  --region ap-northeast-1
```
Kinesis Firehose 可以把 Kinesis Data Streams 中的数据投递到指定存储，目前支持 Redshift、S3、ElasticSearch 和 Splunk，我们这里以 S3 为例。配置前需要定义好 [IAM role](https://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html#using-iam-s3) 并建好 S3 bucket，ARN 的格式可以参考这个[页面](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)。

下面是创建 Firehose 投递流的 CLI 命令示例，请对配置中的 YOUR_ACOUNT_ID、ROLE_NAME 和 BUCKET_NAME 根据实际情况进行替换。
```
echo '''
{
  "RoleARN": "arn:aws:iam::YOUR_ACOUNT_ID:role/ROLE_NAME",
  "BucketARN": "arn:aws:s3:::BUCKET_NAME",
  "Prefix": "source/dms_sample/!{timestamp:yyyy-MM-dd}",
  "ErrorOutputPrefix": "source/errors/!{firehose:error-output-type}-!{timestamp:yyyy-MM-dd}",
  "BufferingHints": {
    "SizeInMBs": 128,
    "IntervalInSeconds": 600
  },
  "CompressionFormat": "GZIP",
  "CloudWatchLoggingOptions": {
    "Enabled": true,
    "LogGroupName": "deliverystream",
    "LogStreamName": "S3Delivery"
  }
}
''' > s3_settings.json

echo '''
{
  "KinesisStreamARN": "arn:aws:kinesis:ap-northeast-1:YOUR_ACOUNT_ID:stream/dms_sample",
  "RoleARN": "arn:aws:iam::YOUR_ACOUNT_ID:role/ROLE_NAME"
}
'''> kinesis_settings.json

aws firehose create-delivery-stream \
  --delivery-stream-name "dms_sample" \
  --delivery-stream-type "KinesisStreamAsSource" \
  --kinesis-stream-source-configuration "file://kinesis_settings.json" \
  --s3-destination-configuration "file://s3_settings.json" \
  --region ap-northeast-1
```

## 2. 配置 DMS 进行数据采集

参考 DMS [产品文档](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_GettingStarted.html)配置好 DMS 复制实例和 MySQL 终端节点，Kinesis 目标终端节点的配置可以参考[这个页面](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html)。复制实例引擎版本务必确定在 3.1.4 及以上。配置完后，验证复制实例到终端节点的连接。

要注意的是，DMS 默认使用单线程向 Kinesis 进行投递，因此我们需要对任务进行配置，增加并发度。下面的设置中，MaxFullLoadSubTasks 设置并发处理 8 张表，ParallelLoadThreads 为 16 表示每张表并发 16 线程进行处理。

下面是创建 DMS 任务的 CLI 命令示例，ARN 在各个组件的详情页，根据实际情况进行替换。
```
echo '''
{
  "TargetMetadata": {
    "ParallelLoadThreads": 16,
    "ParallelLoadBufferSize":500
  },
  "FullLoadSettings": {
    "MaxFullLoadSubTasks": 8,
    "TransactionConsistencyTimeout": 600,
    "CommitRate": 10000
  },
  "Logging": {
    "EnableLogging": true
  },
  "ControlTablesSettings": {
    "ControlSchema":"dms",
    "HistoryTimeslotInMinutes":5,
    "HistoryTableEnabled": true,
    "SuspendedTablesTableEnabled": true,
    "StatusTableEnabled": true
  },
  "ValidationSettings": {
     "EnableValidation": false,
     "ThreadCount": 5
  }
}
''' > task_settings.json

echo '''
{
    "rules": [
        {
            "rule-type": "selection",
            "rule-id": "1",
            "rule-name": "dms_sample-all",
            "object-locator": {
                "schema-name": "dms_sample",
                "table-name": "%"
            },
            "rule-action": "include"
        }
    ]
}
''' > table_mapping.json

aws dms create-replication-task \
  --replication-task-identifier "dmssample-streams" \
  --source-endpoint-arn arn:aws:dms:ap-northeast-1:your_account_id:endpoint:AAAAAAAAAAAAAAAAAAAAAAAAAA \
  --target-endpoint-arn arn:aws:dms:ap-northeast-1:your_account_id:endpoint:AAAAAAAAAAAAAAAAAAAAAAAAAA \
  --replication-instance-arn arn:aws:dms:ap-northeast-1:your_account_id:rep:AAAAAAAAAAAAAAAAAAAAAAAAAA \
  --migration-type "full-load-and-cdc" \
  --table-mappings 'file://table_mapping.json' \
  --replication-task-settings 'file://task_settings.json' \
  --region ap-northeast-1
```

当看到任务状态转为 ready 后，启动任务：
```
aws dms start-replication-task \
  --replication-task-arn arn:aws:dms:ap-northeast-1:your_account_id:task:AAAAAAAAAAAAAAAAAAAAAAAAAA \
  --start-replication-task-type start-replication \
  --region ap-northeast-1
```



在任务详情页，可以查看 DMS 识别并处理的表：



![image3](/Users/haofh/split_kinesis_streams_with_glue/image/image3.png)



## 3. 增加一个 Glue Job 来进行表格分离操作

可以先创建一个 Glue Crawler (可以参考[产品文档](https://docs.aws.amazon.com/glue/latest/dg/console-crawlers.html))，对 Firehose 投递到 S3 中的内容进行爬取，我们可以看到仅有 metadata 和 data 两个字段。



![image1](/Users/haofh/split_kinesis_streams_with_glue/image/image1.png)



每条记录长这个样子：

```
{
	"data":	{
		"id":	2633753,
		"sporting_event_id":	52,
		"sport_location_id":	26,
		"seat_level":	2,
		"seat_section":	"30",
		"seat_row":	"J",
		"seat":	"19",
		"ticket_price":	46.570000
	},
	"metadata":	{
		"timestamp":	"2019-11-13T09:59:08.059607Z",
		"record-type":	"data",
		"operation":	"load",
		"partition-key-type":	"primary-key",
		"schema-name":	"dms_sample",
		"table-name":	"sporting_event_ticket"
	}
}
```
多个表的内容，揉杂在了一起，我们需要通过一个 Glue ETL 任务来进行分离。
Glue 的 Spark 环境支持 Scala 和 Python，下面我们基于 Python 3 来编写代码。为了方便调试，我们可以创建一个 [开发终端节点 和一个 Zeppelin Notebook Server](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint.html)，开发终端节点的权限设置可参考[文档](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)。 当然也可以直接 SSH 到 Development Endpoint 的 [REPL 调试界面](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-repl.html)。

### 3.1 初始化，导入必要的包

以下以 SSH 登录到开发终端节点，在 Python REPL 环境中执行为例：
```
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue import DynamicFrame

# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
```

### 3.2 从 Glue 爬取程序建立的表对象创建一个 DynamicFrame

database 和 table_name 根据 Glue 数据目录中的内容进行修改。
```
# Create a DynamicFrame from AWS Glue Catalog
combined_DyF = glueContext.create_dynamic_frame.from_catalog(database="dms_sample", table_name="source_dms_sample")
```
可以看到现在的数据结构：



![image2](/Users/haofh/split_kinesis_streams_with_glue/image/image2.png)



### 3.3 根据表名进行过滤

我们根据 metadata 中的 schema-name 和 table-name 来过筛选出我们需要的表格 dms_sample.person，因为 Create Table 和 Drop Table 之类的 DDL 语句会生成 data 为空的记录，我们也过滤掉这些记录。
```
# Acquire rows from "person" table
person_DyF = combined_DyF.filter(f = lambda x: \
    x["metadata"]["schema-name"] == "dms_sample" and \
    x["metadata"]["table-name"] == "person" and \
    x["data"] is not None)
```
经过过滤之后的数据结构如下：



![image4](/Users/haofh/split_kinesis_streams_with_glue/image/image4.png)




### 3.4 去掉字段前缀

转换成 PySpark 的 DataFrame， 通过 select 来去掉字段前缀，并且仅保留 data 字段和 metadata 里面的 timestamp 。
```
# Select columns from DataFrame
person_DF = person_DyF.toDF().select(col("data.*"), col("metadata.timestamp"))
```
可以看到现在的表结构已经和我们源表结构相似了（除了我们故意增加的 timestamp 字段）。



- Glue 中的表：

![image5](/Users/haofh/split_kinesis_streams_with_glue/image/image5.png)



- MySQL 中的源表：

![image6](/Users/haofh/split_kinesis_streams_with_glue/image/image6.png)




### 3.5 写入 S3

我们把 DataFrame 转换回 DynamicFrame，然后使用 Parquet 格式写回 S3。为了减少文件的数量，我们通过 repartition 进行了合并。另外，我们使用 gender 作为 partitionKey 展示了目标表分区的功能。当然，在实际使用中，要根据数据量来选择 repartition 的分区数量，防止 OOM；目标表是否分区，分区键的选择也要根据数据分布和查询模式来确定。

S3 路径根据实际情况进行修改。
```
# Write to S3
tmp_dyf = DynamicFrame.fromDF(person_DF.repartition(1), glueContext, "temp")
glueContext.write_dynamic_frame.from_options(\
    tmp_dyf, \
    "s3",\
    {"path": "s3://bucket/target/dms_sample/person/", "partitionKeys": ["first_name"]},\
    "parquet")
```

我们通过另外一个 Glue Crawler 来爬取目标表的结构，现在，我们可以使用 Athena 来对目标表进行查询了：



![image7](/Users/haofh/split_kinesis_streams_with_glue/image/image7.png)




## 4. 总结
在这个 Demo 中，我们把源表中整个 schema 采集到了一个 Kinesis 数据流里面，再利用 AWS Glue 的 filter 筛选出我们需要的表，并充分利用 AWS Glue DynamicFrame schema on-the-fly 的特性，根据当前数据内容，动态生成表结构。

我们看到，AWS Glue 提供了托管的 Spark 集群，还提供了结构爬取、集中元数据存储功能，并且通过 DynamicFrame 对 PySpark 进行了扩展，可以作为我们一站式 ETL 解决方案。