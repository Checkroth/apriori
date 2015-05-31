$SPARK_HOME/bin/spark-submit --class edu.drexel.cs461.apriori \
--conf "spark.executor.uri=hdfs://master:9000/spark/park-1.3.1-bin-hadoop2.6.tgz" \
--conf spark.mesos.coarse=true --executor-memory 6g target/apriori.jar \
hdfs://master:9000/data/apriori/input100 0.02 testout/output100/ local 8
