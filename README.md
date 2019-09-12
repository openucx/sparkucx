# SparkUCX ShuffleManager Plugin
SparkUCX is a high performance ShuffleManager plugin for Apache Spark that uses RDMA and other high performance
transport, supported by [UCX](https://github.com/openucx/ucx#supported-transports) when performing Shuffle data transfers in Spark jobs.

This open-source project is developed, maintained and supported by [UCF consortium](http://www.ucfconsortium.org/).

## Runtime requirements
* Apache Spark 2.2.0/2.3.0/2.4.0
* Java 8+
* An RDMA-supported network, e.g. RoCE or Infiniband

## Installation

### Obtain SparkUCX
Please use the ["Releases"](https://github.com/openucx/sparkucx/releases) page to download pre-built binaries.
<br>If you would like to build the project yourself, please refer to the ["Build"](https://github.com/openucx/sparkucx#build) section below.

ucx binaries **must** be in `java.library.path` on every Spark Master and Worker (usually in /usr/lib).
It can be obtained by installing latest version of [Mellanox OFED](http://www.mellanox.com/page/products_dyn?product_family=26)
or following [ucx build instruction](https://github.com/openucx/ucx#using-ucx).

### Configuration

Provide Spark the location of the SparkUCX plugin jars and ucx shared binaries by using the extraClassPath option.

```
spark.driver.extraClassPath     /path/to/SparkUCX/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:/PATH/TO/UCX/LIB
spark.executor.extraClassPath   /path/to/SparkUCX/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:/PATH/TO/UCX/LIB
```

Add UCX shared binaries to `java.library.path` for Spark driver and executors:
```
spark.driver.extraJavaOptions      -Djava.library.path=/PATH/TO/UCX/LIB
spark.executor.extraJavaOptions    -Djava.library.path=/PATH/TO/UCX/LIB
```

### Running

To enable the SparkUCX Shuffle Manager plugin, add the following configuration:

```
spark.shuffle.manager   org.apache.spark.shuffle.UcxShuffleManager
```

### Build

Building the SparkUCX plugin requires [Apache Maven](http://maven.apache.org/) and Java 8+

1. Install [jucx - java bindings over ucx](https://github.com/openucx/ucx/tree/master/bindings/java)

2. Obtain a clone of [SparkUCX](https://github.com/openucx/sparkucx)

3. Build the plugin for your Spark version (either 2.2.0, 2.3.0, 2.4.0), e.g. for Spark 2.4.0:

```
mvn -DskipTests clean package -Pspark-2.4.0
```
