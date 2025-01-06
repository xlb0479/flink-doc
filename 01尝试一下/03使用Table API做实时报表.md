# 使用Table API做实时报表

Flink提供了一组Table API，作为统一的关系型API，实现批量或流式处理，比如对于无限的实时数据流，或者有限的批量数据集，都能使用同样的查询语义，产出同样的结果。这些Table API主要用于辅助数据分析，数据管道化，以及ETL应用。

## 本文要教你做啥？

在本文中，你会学习如何构建一个实时看板，跟踪金融账户的交易。这条管道会从Kafka读数，然后写到MySQL，最后通过Grafana完成可视化。

## 准备

这里假设你已经熟悉Java语言，但如果你学过其他语言应该也能跟得上。同时假设你了解基本的关系型概念，比如`SELECT`和`GROUP BY`语法。

## 我卡住了怎么办！

如果你卡住了，可以到[社区](https://flink.apache.org/how-to-contribute/getting-help/)寻求帮助。而且，Flink的[用户邮件清单](https://flink.apache.org/what-is-flink/community/)被认为是Apache项目中最活跃的几个之一，可以在此快速获得帮助。

> 如果你是在Windows上跑docker，而用来生成数据的容器又起不来，那么看看你用的shell对不对。比如**table-walkthrough_data-generator_1**这个容器用的**docker-entrypoint.sh**就要用bash。如果没有bash，会抛出**standard_init_linux.go:211: exec user process caused “no such file or directory”**这种错误。一种解决办法是在**docker-entrypoint.sh**的第一行把shell切成**sh**。

## 继续

如果要继续，那么你的电脑上需要安装：

- Java 11
- Maven
- Docker

需要的配置文件在[flink-playgrounds](https://github.com/apache/flink-playgrounds)。下载之后在IDE中打开项目`flink-playground/table-walkthrough`，然后找到`SpendReport`这个文件。

```java
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tEnv = TableEnvironment.create(settings);

tEnv.executeSql("CREATE TABLE transactions (\n" +
    "    account_id  BIGINT,\n" +
    "    amount      BIGINT,\n" +
    "    transaction_time TIMESTAMP(3),\n" +
    "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
    ") WITH (\n" +
    "    'connector' = 'kafka',\n" +
    "    'topic'     = 'transactions',\n" +
    "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
    "    'format'    = 'csv'\n" +
    ")");

tEnv.executeSql("CREATE TABLE spend_report (\n" +
    "    account_id BIGINT,\n" +
    "    log_ts     TIMESTAMP(3),\n" +
    "    amount     BIGINT\n," +
    "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
    ") WITH (\n" +
    "   'connector'  = 'jdbc',\n" +
    "   'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
    "   'table-name' = 'spend_report',\n" +
    "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
    "   'username'   = 'sql-demo',\n" +
    "   'password'   = 'demo-sql'\n" +
    ")");

Table transactions = tEnv.from("transactions");
report(transactions).executeInsert("spend_report");
```

## 代码详解

### 执行环境

头两行创建了`TableEnvironment`。这种table环境可以为你的Job设置各种属性，比如指定你要写的是一个批处理应用还是一个流式应用，以及创建你的source。本文创建了一个标准的table环境，并使用流式处理。

```java
EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

### 注册Table

下一步，要把table注册到当前的[catalog](../04应用开发/03TableAPI和SQL/13Catalogs.md)中，这样才能连接到外部系统读数，并且批量或流式地写出数据。一个table source可以让你访问到外部系统中的数据，比如某个数据库、kv存储、消息队列、以及文件系统。而一个table sink则会在外部系统中产生一个table。根据source和sink的类型不同，它们提供了不同的格式，例如CSV、JSON、Avro、Parquet。

```java
tEnv.executeSql("CREATE TABLE transactions (\n" +
     "    account_id  BIGINT,\n" +
     "    amount      BIGINT,\n" +
     "    transaction_time TIMESTAMP(3),\n" +
     "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
     ") WITH (\n" +
     "    'connector' = 'kafka',\n" +
     "    'topic'     = 'transactions',\n" +
     "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
     "    'format'    = 'csv'\n" +
     ")");
```

我们要注册两个table，一个输入交易的table，一个产出报表的table。交易（`transactions`）table可以读取信用卡交易，其中包含了账户ID（`account_id`）、时间戳（`transaction_time`）、金额（`amount`）。这个table是在Kafka的topic上建立了一个逻辑视图，这个topic就是`transactions`，其中包含了CSV格式的数据。

```java
tEnv.executeSql("CREATE TABLE spend_report (\n" +
    "    account_id BIGINT,\n" +
    "    log_ts     TIMESTAMP(3),\n" +
    "    amount     BIGINT\n," +
    "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
    ") WITH (\n" +
    "    'connector'  = 'jdbc',\n" +
    "    'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
    "    'table-name' = 'spend_report',\n" +
    "    'driver'     = 'com.mysql.jdbc.Driver',\n" +
    "    'username'   = 'sql-demo',\n" +
    "    'password'   = 'demo-sql'\n" +
    ")");
```

第二个table是`spend_report`，保存最终的聚合结果。它底层的存储是MySql数据库中的一张表。

### 把管道建起来

有了上面的环境以及注册的table，已经准备好构建你的第一个应用了。通过`TableEnvironment`你可以从输入table中读取它的一行一行的数据，然后写到输出table中，这里要用到`executeInsert`。`report`函数用来实现你的业务逻辑。现在我们暂且留空。

```java
Table transactions = tEnv.from("transactions");
report(transactions).executeInsert("spend_report");
```

## 测试

项目中包含了一个测试类`SpendReportTest`用来验证报表的逻辑。它创建了一个批处理模式的table环境。

```java
EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
TableEnvironment tEnv = TableEnvironment.create(settings); 
```

Flink的特点之一就是它为批处理和流式处理提供了一致的语义。这就意味着你可以用静态数据集以批处理的形式开发和测试应用，然后到生产环境上部署成流式应用。

## 试一个

现在我们的Job框架已经出来了，可以搞业务逻辑了。我们的目标是做一个报表，统计每个账户每天每小时的总花费。这就需要将时间戳这列从毫秒调整成小时。

Flink在开发关系型应用的时候既可以使用纯[SQL](../04应用开发/03TableAPI和SQL/08SQL.md)，也可以使用[Table API](../04应用开发/03TableAPI和SQL/07Table%20API.md)。Table API是一种流式DSL，也是受SQL启发的，可以用Java或者Python来写，IDE集成度也非常好。和SQL查询类似，Table程序可以select需要的字段，根据key进行group by。这些功能，再加上[内置函数](../04应用开发/03TableAPI和SQL/10函数/02系统（内置）函数.md)，比如`floor`和`sum`，可以让你实现你的报表。

```java
public static Table report(Table transactions) {
    return transactions.select(
            $("account_id"),
            $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
            $("amount"))
        .groupBy($("account_id"), $("log_ts"))
        .select(
            $("account_id"),
            $("log_ts"),
            $("amount").sum().as("amount"));
}
```

## 自定义函数

Flink提供了有限的内置函数，有时你需要扩展出自己的[用户定义函数](../04应用开发/03TableAPI和SQL/10函数/03用户定义函数.md)。假如没有`floor`函数，那你可以自己实现一个。

```java
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class MyFloor extends ScalarFunction {

    public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(
        @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }
}
```

然后可以快速集成到你的应用中。

```java
public static Table report(Table transactions) {
    return transactions.select(
            $("account_id"),
            call(MyFloor.class, $("transaction_time")).as("log_ts"),
            $("amount"))
        .groupBy($("account_id"), $("log_ts"))
        .select(
            $("account_id"),
            $("log_ts"),
            $("amount").sum().as("amount"));
}
```

这个查询会消费掉`transactions`表中的所有数据，计算出报表数据，然后以高效且可扩展的方式写出最终结果。现在你再用这个跑测试就可以通过了。

## 添加Window

在数据处理领域，根据时间分组是一种常见操作，特别是处理无限数据流的时候。根据时间分好的组被称为一个窗口（[window](../04应用开发/02DataStream%20API/06算子/02窗口.md)），Flink提供了灵活的窗口语义。最基本的窗口类型叫做`Tumble`窗口，具备一个固定的大小，每个窗口不会出现重叠。

```java
public static Table report(Table transactions) {
    return transactions
        .window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("log_ts"))
        .groupBy($("account_id"), $("log_ts"))
        .select(
            $("account_id"),
            $("log_ts").start().as("log_ts"),
            $("amount").sum().as("amount"));
}
```

此时你的应用在时间戳列上创建了一个1小时的滚动窗口。此时如果一行数据的时间戳是`2019-06-01 01:23:47`，就会被放到`2019-06-01 01:00:00`这个窗口中。

基于时间的聚合有其特殊性，因为时间与其他属性不同，往往是在流式应用中不断向前移动的。和`floor`以及你的自定义函数不同，窗口函数是内置函数，可以在运行时增加额外的优化。在批处理环境下，窗口函数可以用来根据时间戳对记录进行分组，是一种非常方便的API。

此时继续跑测试，这种实现依然没有问题。