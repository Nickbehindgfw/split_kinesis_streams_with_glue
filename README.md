使用 AWS Glue 从 Kinesis 数据流中分离出数据库表格
================================================
关系型数据库是数据分析过程中非常普遍的一个数据源。一般我们会通过ETL过程，将数据库中的数据采集并转换为我们需要的格式，再由后端分析工具产生我们需要的结果。

在现代数据仓库架构中，我们推荐基于 Amazon Simple Storage(S3)  的数据湖体系结构，AWS Database Migration Service(DMS) 能帮助我们完成关系型数据库到 S3 的全量和增量数据采集。其操作过程非常简单：
1. 准备 DMS 环境，包括创建 VPC、子网、IAM 角色和安全组，创建 DMS 子网组；
2. 创建 DMS 复制实例，因为 DMS 需要缓存从任务开始时起的数据库变更，所以预留好内存和硬盘应对需要。生产环境下，建议启用 Multi-AZ 保证 DMS 的高可用；
3. 建立指向源数据库和 S3 的 Endpoints；
4. 创建并启动迁移任务，数据库记录就会源源不断的进入S3。

DMS 会按每个表一个目录的方式，把数据库记录存储为 CSV 或 Parquet 格式的 S3 对象。AWS 的 ETL 工具 AWS Glue 可以通过爬虫程序爬取表结构，存储在统一的元数据存储——数据目录中，供各种分析工具调用，比如说，使用 Amazon Athena 或者 Amazon Redshift Spectrum 进行即席查询。

有些时候，我们希望更加迅速的访问到数据库的变更内容，而通过 S3 中转，增加了处理时延，不符合我们的性能需求。这个时候，我们会引入流处理框架。

Amazon Kinesis Data Streams 是在 Amazon 内部和外部都得到广泛使用的流式存储引擎。我们通过 Amazon Kinesis Data Streams，把数据表通过 Kinesis 转化为数据流。不过这种情况下，如果我们想复用这个数据流，进行批式数据处理，会遇到一些问题。当我们通过 Amazon Kinesis Firehose 把数据投递到 S3 后，我们会发现整个流的数据被放置在同一个文件夹下，而且数据是JSON格式，每条记录中包含metadata和data两个一级元素。AWS Glue 的结构爬取程序对记录结构进行解析后，会仅识别为一张只有两个字段的大表。

如果让每个表格使用独立的数据流，可以解决上述问题，但增加了管理难度。如果另起一个 DMS 进程，则会增加源库负担。是否有更其它方法呢？其实我们可以借助 Glue 对 PySpark 语法的扩展，来灵活处理此问题。

具体来讲，就是使用 filter 这个 Transform 方法，基于 metadata 中的 schema name + table name 对记录进行过滤，把不同的表格内容分离出来。

接下来，我们将通过一个 Demo 来演示具体操作，假设您已经安装并正确设置了 [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)，并使用 AWS 东京区域（ap-northeast-1）。

## 1. 新建 Kinesis Data Streams 数据流和 Firehose 投递流
Kinesis Data Streams 的创建非常简单，提供 stream 名称和 shard 数量即可
```
aws kinesis create-stream \
  --stream-name "employees" \
  --shard-count 2 \
  --region ap-northeast-1
```
Kinesis Firehose 支持投递到 Redshift、S3、ElasticSearch 和 Splunk，我们这里以 S3 为例。配置前需要定义好 IAM role 并建好 S3 bucket，ARN 的格式可以参考这个[页面](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)，对配置中的 your_account_id、role_name 和 bucket_name 根据实际情况进行替换。
```
echo '''
{
  "RoleARN": "arn:aws:iam::your_acount_id:role/role_name",
  "BucketARN": "arn:aws:s3:::bucket_name",
  "Prefix": "source/employees/!{timestamp:yyyy-MM-dd}",
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
  "KinesisStreamARN": "arn:aws:kinesis:ap-northeast-1:your_account_id:stream/employees",
  "RoleARN": "arn:aws:iam::your_account_id:role/role_name"
}
'''> kinesis_settings.json

aws firehose create-delivery-stream \
  --delivery-stream-name "employees" \
  --delivery-stream-type "KinesisStreamAsSource" \
  --kinesis-stream-source-configuration "file://kinesis_settings.json" \
  --s3-destination-configuration "file://s3_settings.json"
```

## 2. 配置 DMS 进行数据采集
DMS 的配置参考[Using Amazon Kinesis Data Streams as a Target for AWS Database Migration Service
](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html)。要注意的是，DMS 默认使用单线程向 Kinesis 进行投递，因此我们需要对任务进行配置，增加并发度。我们假设您已经正确创建了 Replication instance 和 Endpoints，并经测试可以成功连接。
在示例代码中，我们从一个 MySQL 版本的 RDS 实例，进行全量和增量的数据抽取，通过 MaxFullLoadSubTasks 设置并发处理 8 张表，ParallelLoadThreads 为 16 表示每张表并发 16 线程进行处理。需要提醒的是，MySQL Binlog 的格式必须为 Row （默认 Parameter Group 不可更改，更换 Parameter Group 需要手动重启实例方可生效），并且合理设置了日志保留时间（ retention hours），设置方式可以参考[这里](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.Concepts.MySQL.html)。
ARN 在各个组件的详情页，根据实际情况进行替换。
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
            "rule-name": "schema-employees-all",
            "object-locator": {
                "schema-name": "employees",
                "table-name": "%"
            },
            "rule-action": "include"
        }
    ]
}
''' > table_mapping.json

aws dms create-replication-task \
  --replication-task-identifier "employees-steams" \
  --source-endpoint-arn arn:aws:dms:ap-northeast-1:your_account_id:endpoint:AAAAAAAAAAAAAAAAAAAAAAAAAA \
  --target-endpoint-arn arn:aws:dms:ap-northeast-1:your_account_id:endpoint:AAAAAAAAAAAAAAAAAAAAAAAAAA \
  --replication-instance-arn arn:aws:dms:ap-northeast-1:your_account_id:rep:AAAAAAAAAAAAAAAAAAAAAAAAAA \
  --migration-type "full-load-and-cdc" \
  --table-mappings 'file://table_mapping.json' \
  --replication-task-settings 'file://task_settings.json' 
```
当看到任务状态转为 ready 后，启动任务：
```
aws dms start-replication-task \
  --replication-task-arn arn:aws:dms:ap-northeast-1:your_account_id:task:AAAAAAAAAAAAAAAAAAAAAAAAAA \
  --start-replication-task-type start-replication
```

## 3. 增加一个 Glue Job 来进行表格分离操作
可以先创建一个 Glue Crawler ，对 Firehose 投递到 S3 中的内容进行爬取，我们可以看到仅有 metadata 和 data 两个字段。
![schema_of_source](https://github.com/Nickbehindgfw/split_kinesis_stream_with_glue/raw/master/image/image1.png)  
每条记录长这个样子：
```
{
	"data":	{
		"emp_no":	67147,
		"birth_date":	"1959-04-05",
		"first_name":	"Yucai",
		"last_name":	"Krider",
		"gender":	"F",
		"hire_date":	"1990-12-07"
	},
	"metadata":	{
		"timestamp":	"2019-10-09T12:04:00.209212Z",
		"record-type":	"data",
		"operation":	"load",
		"partition-key-type":	"primary-key",
		"schema-name":	"employees",
		"table-name":	"employees"
	}
}
```
多个表的内容，揉杂在了一起，我们需要通过一个 Glue ETL 任务来进行分离，Glue 支持 Scala 和 Python，下面我们基于 Python 3.0 来编写 ETL 代码，为了方便调试，我们可以创建一个 Development Endpoint 和一个 Zeppelin Notebook Server。 

### 3.1 初始化，导入必要的包
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
```
# Create a DynamicFrame from AWS Glue Catalog
combined_DyF = glueContext.create_dynamic_frame.from_catalog(database="source", table_name="employees")
```

### 3.3 根据表名进行过滤
我们根据 metadata 中的 schema-name 和 table-name 来过筛选出我们需要的表格 employees.employees，因为 Create Table 和 Drop Table 之类的 DDL 语句会生成 data 为空的记录，我们也过滤掉这些记录。
```
# Acquire rows from "employees" table
employees_DyF = combined_DyF.filter(f = lambda x: \
    x["metadata"]["schema-name"] == "employees" and \
    x["metadata"]["table-name"] == "employees" and \
    x["data"] is not None)
```

### 3.4 去掉字段前缀
转换成 PySpark 的 DataFrame， 通过 select 来去掉字段前缀，并且仅保留 data 字段和 metadata 里面的 timestamp 。
```
employees_DF = employees_DyF.toDF().select(col("data.*"), col("metadata.timestamp"))
```

### 3.5 写入 S3
我们把 DataFrame 转换回 DynamicFrame，然后使用 Parquet 格式写回 S3。为了减少文件的数量，我们通过 repartition 进行了合并。另外，我们使用 gender 作为 partitionKey 展示了目标表分区的功能。当然，在实际使用中，要根据数据量来选择 repartition 的分区数量，防止 OOM；目标表是否分区，分区键的选择也要根据数据分布和查询模式来确定。
```
# Write to S3
tmp_dyf = DynamicFrame.fromDF(employees_DF.repartition(1), glueContext, "temp")
glueContext.write_dynamic_frame.from_options(\
    tmp_dyf, \
    "s3",\
    {"path": "s3://bucket/target/employees/employees/", "partitionKeys": ["gender"]},\
    "parquet")
```

我们通过另外一个 Glue Crawler 来爬取目标表的结构，现在，我们可以使用 Athena 来对目标表进行查询了。

## 4. 总结
在这个 Demo 中，我们把源表中整个 schema 采集到了一个 Kinesis 数据流里面，再利用 AWS Glue 的 filter 筛选出我们需要的表，并充分利用 AWS Glue DynamicFrame schema on-the-fly 的特性，根据当前数据内容，动态生成表结构。

我们看到，AWS Glue 除了提供了托管的 Spark 集群来承载 ETL 任务外，还提供了结构爬取程序、集中元数据存储，并且通过 DynamicFrame 对 PySpark 进行了扩展，满足了开发中的功能需求。


