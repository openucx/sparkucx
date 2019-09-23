# SparkUCX ShuffleManager Plugin
SparkUCX is a high performance ShuffleManager plugin for Apache Spark that uses RDMA and other high performance
transport, supported by [UCX](https://github.com/openucx/ucx#supported-transports) when performing Shuffle data transfers in Spark jobs.

This open-source project is developed, maintained and supported by [UCF consortium](http://www.ucfconsortium.org/).

## Runtime requirements
* Apache Spark 2.3/2.4
* Java 8+
* Installed UCX of version 1.7+, and [UCX supported transport hardware](https://github.com/openucx/ucx#supported-transports).

## Installation

### Obtain SparkUCX
Please use the ["Releases"](https://github.com/openucx/sparkucx/releases) page to download SparkUCX jar file
for your spark version (e.g. spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar).
Put SparkUCX jar file to $SPARK_UCX_HOME on all the nodes in your cluster.
<br>If you would like to build the project yourself, please refer to the ["Build"](https://github.com/openucx/sparkucx#build) section below.

Ucx binaries **must** be in `java.library.path` on every Spark Master and Worker.
It can be obtained by installing latest version of [Mellanox OFED](http://www.mellanox.com/page/products_dyn?product_family=26)
or following [ucx build instruction](https://github.com/openucx/ucx#using-ucx). E.g.:

```
% export UCX_PREFIX=/usr/local
% git clone https://github.com/openucx/ucx.git
% cd ucx
% ./contrib/configure-release --with-java –-prefix=$UCX_PREFIX
% make -j`nproc` && make install
```

### Configuration

Provide Spark the location of the SparkUCX plugin jars and ucx shared binaries by using the extraClassPath option.

```
spark.driver.extraClassPath     $SPARK_UCX_HOME/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:$UCX_PREFIX/lib
spark.executor.extraClassPath   $SPARK_UCX_HOME/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:$UCX_PREFIX/lib
```

Add UCX shared binaries to `java.library.path` for Spark driver and executors:
```
spark.driver.extraJavaOptions      -Djava.library.path=$UCX_PREFIX/lib
spark.executor.extraJavaOptions    -Djava.library.path=$UCX_PREFIX/lib
```

To enable the SparkUCX Shuffle Manager plugin, add the following configuration property
to spark (e.g. $SPARK_HOME/conf/spark-defaults.conf):

```
spark.shuffle.manager   org.apache.spark.shuffle.UcxShuffleManager
```

### Build

Building the SparkUCX plugin requires [Apache Maven](http://maven.apache.org/) and Java 8+ JDK

Build instructions:

```
% git clone https://github.com/openucx/sparkucx
% cd sparkucx
% mvn -DskipTests clean package -Pspark-2.3
```

