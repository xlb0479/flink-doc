# DataStream API介绍

本节为你整体介绍以下DataStream API，让你能开始编写流式应用。

## 什么东西可以被流式处理？

Flink的DataStream API可以让你对任何可以序列化的东西进行流式处理。Flink自带的序列化工具可以用来处理

- 基本类型，例如String、Long、Integer、Boolean、数组
- 复合类型：Tuple、POJO、以及Scala的case类

对于其他类型Flink则直接使用Kryo。也可以使用其他的序列化工具，比如对Avro的支持就非常好。

### Java的Tuple和POJO

Flink自带的序列化工具可以很好的处理Tuple和POJO。

#### Tuple

对于Java，Flink预置了从`Tuple0`到`Tuple25`这些类。

```java
Tuple2<String, Integer> person = Tuple2.of("Fred", 35);

// zero based index!  
String name = person.f0;
Integer age = person.f1;
```

#### POJO

如果满足下面的条件，Flink就会把它当作一个POJO（允许字段名引用）：

- 类是public的，并且只有一层（没有非静态内部类）
- 类有public的无参构造方法
- 类（以及所有父类）中所有非静态，非transient字段要么是public的（且非final），要么有public的getter和setter，而且符合Java bean的命名规范。

例如：

```java
public class Person {
    public String name;  
    public Integer age;  
    public Person() {}
    public Person(String name, Integer age) {  
        . . .
    }
}  

Person person = new Person("Fred Flintstone", 35);
```

Flink的序列化[支持POJO类型的模式演化](../04应用开发/02DataStream%20API/04状态和容错/05数据类型和序列化/02状态模式演化.md#支持模式演化的类型)。

### Scala的tuple和case类

这里没什么好说的，跟你想的一样。

> Flink所有的Scala API都处于deprecated，未来版本中就会去掉。目前你仍然可以用Scala来构建应用，但不管是DataStream还是Table的API你都应该开始向Java版本迁移了。<br/><br/>见[FLIP-265 Deprecate and remove Scala API support](https://cwiki.apache.org/confluence/display/FLINK/FLIP-265+Deprecate+and+remove+Scala+API+support)

## 完整示例

下面这个例子以人员信息作为输入流，从中过滤出成年人。

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;

public class Example {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));

        DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });

        adults.print();

        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;
        public Person() {}

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        }
    }
}
```

### 流的执行环境

每个Flink应用都需要它的执行环境，本例中就是那个`env`。流式应用自然要用`StreamExecutionEnvironment`。

代码中使用DataStream的API，在`StreamExecutionEnvironment`上构建了一个作业图。当`env.execute()`被调用的时候，这个作业图就会被打包发送给JobManager，然后会被切成片分发给TaskManager。每个并行的切片会被放到一个*task slot*中执行。

注意如果不调用execute()，应用是跑不起来的。

![02学习Flink-02DataStream API介绍-01.png](img/02学习Flink-02DataStream%20API介绍-01.png)

这种分布式运行时依赖于应用的序列化。同时要求集群中每个节点都能访问到所有的依赖。

### 基础source

上面的代码中用`env.fromElements(...)`构造了一个`DataStream<Person>`。对于原型设计或测试来说这是一种很简单的方法。`StreamExecutionEnvironment`还有一个`fromCollection(Collection)`方法。可以这样用：

```java
List<Person> people = new ArrayList<Person>();

people.add(new Person("Fred", 35));
people.add(new Person("Wilma", 35));
people.add(new Person("Pebbles", 2));

DataStream<Person> flintstones = env.fromCollection(people);
```

设计原型时也可以用socket来快速注入数据

```java
DataStream<String> lines = env.socketTextStream("localhost", 9999);
```

或者用文件

```java
DataStream<String> lines = env.readTextFile("file:///path");
```

在实际应用中，常用的source都需要支持低延迟高吞吐并行读，以及回退和重放——这也是高性能和容错性的前提——比如Kafka、Kinesis还有一些其他的文件系统。REST API和各种数据库也经常用到，丰富了流的设计。

### 基础sink

上面代码调用了`adults.print()`，将结果打印到了TaskManager的日志中（如果是在IDE运行的话就是打到了IDE的控制台里）。它会调用每个元素的`toString()`。

输出类似这样

```console
1> Fred: age 35
2> Wilma: age 35
```

1>和2>表示这条结果是哪个子任务（或线程）产生的。

在生产环境中，常用的sink包括FileSink、各种数据库，以及pub-sub系统。

### 调试

在生产环境上，你的应用是跑在远程集群或容器中的。如果出现异常，那也是在远端发生的。JobManager和TaskManager的日志对于这些异常的排查非常有用，而如果是在IDE中进行本地调试就更简单了，Flink也支持这么做。可以设置断点、检查变量、单步调试。也可以跳入到Flink源码中，这样可以更详细的了解内部机制。

## 实战练习

现在你拥有了足够的知识，可以开始写一个简单的DataStream应用了。可以拉一下[flink-training-repo](https://github.com/apache/flink-training/tree/release-1.20/)这个代码，按照README中的指引操作，完成第一个练习：[流的过滤（车程清理）](https://github.com/apache/flink-training/blob/release-1.20//ride-cleansing)。