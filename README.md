# SparkUCX ShuffleManager Plugin
SparkUCX is a high performance ShuffleManager plugin for Apache Spark, that uses RDMA and other high performance transports
that are supported by [UCX](https://github.com/openucx/ucx#supported-transports), to perform Shuffle data transfers in Spark jobs.

This open-source project is developed, maintained and supported by the [UCF consortium](http://www.ucfconsortium.org/).

## Runtime requirements
* Apache Spark 2.3/2.4/3.0/3.1
* Java 8+
* Installed UCX of version 1.10+, and [UCX supported transport hardware](https://github.com/openucx/ucx#supported-transports).

## Installation

### Obtain SparkUCX
Please use the ["Releases"](https://github.com/openucx/sparkucx/releases) page to download SparkUCX jar file
for your spark version (e.g. spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar).
Put SparkUCX jar file in $SPARK_UCX_HOME on all the nodes in your cluster.
<br>If you would like to build the project yourself, please refer to the ["Build"](https://github.com/openucx/sparkucx#build) section below.

Ucx binaries **must** be in Spark classpath on every Spark Master and Worker.
It can be obtained by installing the latest version from [Ucx release page](https://github.com/openucx/ucx/releases)

### Configuration

Provide Spark the location of the SparkUCX plugin jars and ucx shared binaries by using the extraClassPath option.

```
spark.driver.extraClassPath     $SPARK_UCX_HOME/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:$UCX_PREFIX/lib
spark.executor.extraClassPath   $SPARK_UCX_HOME/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:$UCX_PREFIX/lib
```
To enable the SparkUCX Shuffle Manager plugin, add the following configuration property
to spark (e.g. in $SPARK_HOME/conf/spark-defaults.conf):

```
spark.shuffle.manager   org.apache.spark.shuffle.UcxShuffleManager
```
For spark-3.0 or spark-3.1 versions add SparkUCX ShuffleIO plugin:
```
spark.shuffle.sort.io.plugin.class org.apache.spark.shuffle.compat.spark_(3_0|3_1).UcxLocalDiskShuffleDataIO
```

### Build

Building the SparkUCX plugin requires [Apache Maven](http://maven.apache.org/) and Java 8+ JDK

Build instructions:

```
% git clone https://github.com/openucx/sparkucx
% cd sparkucx
% mvn -DskipTests clean package -Pspark-2.4
```

### Performance

SparkUCX plugin is built to provide the best performance out-of-the-box, and provides multiple configuration options to further tune SparkUCX per-job. For more information on how to setup [HiBench](https://github.com/Intel-bigdata/HiBench) benchmark and reproduce results, please refer to [Accelerated Apache SparkUCX 2.4/3.0 cluster deployment](https://docs.mellanox.com/pages/releaseview.action?pageId=19819236).

![Performance results](https://docs.mellanox.com/download/attachments/19819236/image2020-1-23_15-39-14.png)

