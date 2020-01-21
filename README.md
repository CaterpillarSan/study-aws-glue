# study-aws-glue

## 準備
see: https://docs.aws.amazon.com/ja_jp/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-scala

```
$ curl -O https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz
$ tar zxvf apache-maven-3.6.0-bin.tar.gz -C /path/to/maven
$ export PATH=/path/to/maven/apache-maven-3.6.0/bin:$PATH

$ curl -O https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz
$ tar zxvf spark-2.4.3-bin-hadoop2.8.tgz -C /path/to/spark
$ export SPARK_HOME=/path/to/spark/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8
```

## 実行
```
$ mvn package
$ mvn exec:java -Dexec.mainClass="HelloWorld" -Dexec.args="--JOB-NAME HelloWorld"
$ mvn exec:java -Dexec.mainClass="SparkSample" -Dexec.args="--JOB-NAME SparkSample"
```
