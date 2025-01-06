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

Flink中最基本的状态类型是[ValueState](../04应用开发/02DataStream%20API/04状态和容错/01状态.md)，它可以对其包装的各种类型提供容错能力。`ValueState`格式属于 *`keyed state`* ，也就是说它只能应用于 *`keyed context`* 中的算子，算子是紧跟着`DataStream#keyBy`。算子的 *`keyed state`* 会自动划归到当前正在处理记录的key。在本例中，key就是当前交易的账户id（它是通过` keyBy()`声明的），`FraudDetector`为每个账户维护了一个独立的状态。`ValueState`是用`ValueStateDescriptor`创建的，其中包含了一些元数据，Flink用这些元数据来管理这些变量。状态的注册发生于数据开始处理之前。也就是要使用下面说的`open()`方法作为钩子。

```java
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Boolean> flagState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
    }
```

`ValueState`属于包装类，类似Java标准库中的`AtomicReference`或`AtomicLong`。它提供了三个用来操作数据的方法，`update`可以设置状态，`value`读取当前状态，`clear`则清空内容。如果一个key的状态是空的，比如应用刚启动的时候，或者调用了`ValueState#clear`，那么`ValueState#value`返回的就是`null`。如果对`ValueState#value`方法返回的对象进行修改，这种修改无法确保能被系统感知到，因此所有的修改必须要用`ValueState#update`。除了这种需要特别指出的情况外，容错功能都是Flink底层提供的，你使用的时候就跟普通变量没什么区别。

下面你可以看到如何使用一个标记状态来跟踪潜在的欺诈交易。

```java
@Override
public void processElement(
        Transaction transaction,
        Context context,
        Collector<Alert> collector) throws Exception {

    // Get the current state for the current key
    Boolean lastTransactionWasSmall = flagState.value();

    // Check if the flag is set
    if (lastTransactionWasSmall != null) {
        if (transaction.getAmount() > LARGE_AMOUNT) {
            // Output an alert downstream
            Alert alert = new Alert();
            alert.setId(transaction.getAccountId());

            collector.collect(alert);            
        }

        // Clean up our state
        flagState.clear();
    }

    if (transaction.getAmount() < SMALL_AMOUNT) {
        // Set the flag to true
        flagState.update(true);
    }
}
```

对于每笔交易都会检查该账户的标记。注意`ValueState`始终是关联到当前key的，也就是这里的账户。如果标记非空，那么该账户的上一笔交易就是小额交易，并且如果当前交易是大额交易，那么就要给出一个欺诈告警。

检查过后自然要清空标记状态，不论当前交易是否触发欺诈告警。

最后，还要检查当前交易是否是小额交易。如果是，那么就要设置标记，给下一次事件提供依据。注意`ValueState<Boolean>`包含了三种状态，未设置（`null`）、`true`、`false`，因为所有的`ValueState`都是nullable的。这个例子只使用了未设置（`null`）和`true`，只判断有没有值。

## 版本v2：状态+时间=❤️

犯罪分子对于他们要进行的大额交易不会等太久，以免他们之前做的验证性的交易被发现。那么你可以在欺诈检测中设置一个1分钟的超时时间，对于前面的例子来说，交易3和4只有在相距1分钟之内发生才会被认定为欺诈。Flink的`KeyedProcessFunction`允许你设置定时器，在之后的某个时间触发一个回调方法。

现在我们看一下要怎么修改：

- 当标记值被设置为`true`时，同时启动一个1分钟的定时器。
- 定时器到点时，清空状态以重置标记。
- 如果标记已经被清除了，那么定时器也要取消掉。

如果要取消一个定时器，需要记住它所设定的时间，要记住也就意味着需要一个状态，所以我们先来创建一个定时器状态，和标记状态在一起。

```java
private transient ValueState<Boolean> flagState;
private transient ValueState<Long> timerState;

@Override
public void open(OpenContext openContext) {
    ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
            "flag",
            Types.BOOLEAN);
    flagState = getRuntimeContext().getState(flagDescriptor);

    ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
            "timer-state",
            Types.LONG);
    timerState = getRuntimeContext().getState(timerDescriptor);
}
```

`KeyedProcessFunction#processElement`调用时它的`Context`中包含了一个定时器服务。这个定时器服务可以用来查询当前时间、注册的定时器，还可以删除定时器。有了它，每次设置标记的时候你就可以设置一个未来1分钟的定时器，并且将时间戳保存在`timerState`中。

```java
if (transaction.getAmount() < SMALL_AMOUNT) {
    // set the flag to true
    flagState.update(true);

    // set the timer and timer state
    long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
    context.timerService().registerProcessingTimeTimer(timer);
    timerState.update(timer);
}
```

其中的processing time是现实世界实际时间，由算子所在机器的系统所决定。

当定时器触发时，它会调用`KeyedProcessFunction#onTimer`。覆盖这个方法可以实现你的回调逻辑，重置标记。

```java
public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
    // remove flag after 1 minute
    timerState.clear();
    flagState.clear();
}
```

最后，要取消这个定时器，需要删除已注册的定时器，并且删除定时器状态。可以把这部分逻辑封到一个单独的方法里一次性调用。

```java
private void cleanUp(Context ctx) throws Exception {
    // delete timer
    Long timer = timerState.value();
    ctx.timerService().deleteProcessingTimeTimer(timer);

    // clean up all state
    timerState.clear();
    flagState.clear();
}
```

好了现在完成了，一个功能齐全、有状态、分布式的流处理应用！

## 最终代码

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();

        // Check if the flag is set
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                //Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }
            // Clean up our state
            cleanUp(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);

            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // remove flag after 1 minute
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }
}
```

### 示例输出

使用`TransactionSource`这个source来运行上面的代码，会对账户3给出欺诈告警。在你的task manager中会看到下面的日志：

```log
2019-08-19 14:22:06,220 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2019-08-19 14:22:11,383 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2019-08-19 14:22:16,551 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2019-08-19 14:22:21,723 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
2019-08-19 14:22:26,896 INFO  org.apache.flink.walkthrough.common.sink.AlertSink - Alert{id=3}
```