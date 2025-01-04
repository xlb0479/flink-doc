# 使用DataStream API进行欺诈检测

Flink提供了一套DataStream API，用于构建健壮且有状态的流式应用。它可以对状态及时间进行细粒度的管控，从而可以实现高级的事件驱动系统。在本文中你会学习如何使用Flink的DataStream API构建一个有状态的流式应用。

## 你要构建的是个什么东西呢？

在数字领域，信用欺诈问题越来越严重。犯罪分子通过欺诈手段，或者进行系统入侵，盗取信用卡号。被盗的信用卡号会经过几次小额验证，通常小于1美元。如果能用，他们就会用它来购买一些可以售卖的大额物品，或者留作自用。

在本文中，你会构建一个欺诈检测系统，发现可疑的信用卡交易。通过一系列简单的规则，你能发现Flink可以帮我们实现复杂的业务逻辑，并且可以进行实时计算。

## 准备工作

我们假设你熟悉Java语言，但即便你只会其他语言，应该也能看得懂。

### 在IDE中运行

如果在IDE中运行项目可能会出现`java.lang.NoClassDefFoundError`异常。这通常是因为classpath中缺少Flink依赖。

> IntelliJ IDEA：Run > Edit Configurations > Modify options > 选择include dependencies with "Provided" scope。然后IDE中运行的应用就会包含所有必要的类。

## 帮我一下，我卡住了！

如果你卡住了，可以到[社区](https://flink.apache.org/how-to-contribute/getting-help/)寻求帮助。而且，Flink的[用户邮件清单](https://flink.apache.org/what-is-flink/community/)被认为是Apache项目中最活跃的几个之一，可以在此快速获得帮助。

## 继续

如果你要继续，那么你的电脑中需要安装：

- Java11
- Maven

使用Flink Maven Archetype可以快速创建一个基本的项目，包含所有需要的依赖，这样你只需要关注业务逻辑即可。这些依赖中包括`flink-streaming-java`，它是所有Flink流式应用的核心依赖，以及`flink-walkthrough-common`，包含了数据生成以及本文中需要的其他类。

```shell
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \
    -DarchetypeVersion=1.20.0 \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false
```

`groupId`、`artifactId`、`package`这些你都可以改。有了上面这串命令，Maven会创建一个名为`frauddetection`的目录，里面的项目包含了本文所需的全部依赖。将项目导入到你的编辑器后，里面有一个`FraudDetectionJob.java`文件，代码如下，你可以直接在IDE中运行。你可以添加一些断点然后用DEBUG模式运行，便于你了解整个流程。

### FraudDetectionJob.java

```java
package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
            .addSource(new TransactionSource())
            .name("transactions");
        
        DataStream<Alert> alerts = transactions
            .keyBy(Transaction::getAccountId)
            .process(new FraudDetector())
            .name("fraud-detector");

        alerts
            .addSink(new AlertSink())
            .name("send-alerts");

        env.execute("Fraud Detection");
    }
}
```

### FraudDetector.java

```java
package spendreport;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
    }
}
```

## 代码详解

现在我们一步一步看一下这些代码。`FraudDetectionJob`这个类定义了应用的数据流，`FraudDetector`则定义了欺诈交易的检测逻辑。

我们先看一下`FraudDetectionJob`的`main`方法。

### Execution Environment

第一行创建了`StreamExecutionEnvironment`。执行环境（execution environment）指的是你如何为作业设置属性、创建source，以及如何启动作业。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

### 创建一个Source

Source从外部系统注入数据，比如Kafka、RabbitMQ、Pulsar，通过它们将数据注入到Flink作业中。此处我们创建了一个可以无限生成信用卡交易流的source。每个交易包含了一个账户ID（`accountId`），交易时间戳（`timestamp`），以及交易金额（`amount`）。给source设置的`name`只是为了便于调试，这样如果有什么问题，我们就能知道问题是哪里产生的。

```java
DataStream<Transaction> transactions = env
    .addSource(new TransactionSource())
    .name("transactions");
```

### 事件分区与欺诈检测

`transactions`流中包含了来自大量用户的大量交易，因此需要通过多个欺诈检测任务来并行处理。因为欺诈的发生是基于每个账户来说的，所以你需要保证同一个账户的所有交易必须被同一个并行任务处理，这些并行任务我们把它们称为欺诈检测算子（operator）。

为了保证同一个任务能够处理特定key的全部记录，可以用`DataStream#keyBy`对数据流进行分区。`process()`方法为分区内的每一个元素调用一个算子。对于这种在`keyBy`后跟着一个算子的情况，我们这里的算子是`FraudDetector`，我们称它运行在一个 *`keyed context`* 中。

```java
DataStream<Alert> alerts = transactions
    .keyBy(Transaction::getAccountId)
    .process(new FraudDetector())
    .name("fraud-detector");
```

### 输出

Sink可以将`DataStream`写出到外部系统中，比如Kafka、Cassandra、AWS Kinesis。这里的`AlertSink`通过**INFO**级别的日志将`Alert`输出，并没有写到外部存储系统，这样也便于你观察结果。

```java
alerts.addSink(new AlertSink());
```

### 欺诈检测算子

欺诈检测实现了一个`KeyedProcessFunction`。每一笔交易事件都会调用`KeyedProcessFunction#processElement`。我们目前就先对每一次交易生成一个警告，看上去可能有点保守哈哈。

后面我们会带着你细化欺诈检测的业务逻辑。

```java
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {
  
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());

        collector.collect(alert);
    }
}
```

## 现在来搞得真实一点（v1）

对于第一版，欺诈检测需要发现一个小额交易后紧跟着一个大额交易的情况，并给出警告。小额定义为小于1美元，大额则定义为大于500美元。假设你的欺诈检测处理的是下面这样一个交易流，它们都是同一个特定账户的交易。

![01尝试一下-01开始-05.png](01尝试一下-01开始-05.png)

交易3和4应当被标记成欺诈，因为它就是一个小额交易0.09美元，后面紧跟着一个大额交易510美元。而对于交易7、8、9，则不是欺诈，因为0.02美元的小额交易后面没有紧跟着的大额交易，中间有一笔交易打破了这个模式设定。

为了实现这个功能，欺诈检测需要在多个事件之间做到*remember*。大额交易只有前一个是小额交易的情况下才能标记为欺诈。记忆信息就需要[状态(state)](../03概念/04术语.md#managed-state)，这也是我们为什么要用[KeyedProcessFunction](../04应用开发/02DataStream%20API/06算子/04ProcessFunction.md)。它对于状态和时间都提供了细粒度的控制能力，使我们的算法得以改进，满足更加复杂的需要。

最直接的实现方式就是用一个布尔标记，遇到小额交易就标记一下。遇到大额交易的时候，就可以检查该账户下这个标记的状态。

但如果只是把这个标记作为`FraudDetector`的一个成员变量是不行的。对于来自多个账户的交易，Flink使用了同一个`FraudDetector`对象实例，那么如果账户A和账户B的交易被路由到了同一个`FraudDetector`实例，账户A的一笔交易可能将标记设置为true，然后账户B的一笔交易就可能会导致一个误报。当然，我们可以用一个`Map`结构来跟踪每个key的标记情况，但是一个成员变量的容错能力太差，一旦出现问题，它的信息可能就全部丢失了。因此，当应用程序从异常情况下恢复时，我们的欺诈检测程序就可能出现错误。

为了解决这个问题，Flink提供了一些容错状态原语，用起来跟普通的成员变量没什么差别。

Flink中最基本的状态类型是[ValueState](../04应用开发/02DataStream%20API/04状态和容错/01状态.md)，它可以对其包装的各种类型提供容错能力。