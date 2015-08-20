#!/bin/sh
spark_path=/home/resys/var/spark-1.4.1-bin-hadoop2.4/bin
ip=172.16.1.7
function compile() {
    rsync -C -apr --inplace --progress --partial -e "ssh -c arcfour" gaoyunxiang@$ip:~/workspace/exp/NaiveBayes/src . || return  1
    sbt assembly
}
function bayes_stat() {
    training_input=/gaoyunxiang/model_merge/training_output
    bayes_pz_output=/gaoyunxiang/model_merge/bayes_pz_output
    bayes_pwz_output=/gaoyunxiang/model_merge/bayes_pwz_output
    hdfs dfs -rm -r $bayes_pz_output
    hdfs dfs -rm -r $bayes_pwz_output
    $spark_path/spark-submit  --name "model bayes stat @_@gaoyunxiang" --class "Bayes" --master yarn-cluster  --num-executors 400  --driver-memory 60g  --executor-memory 10g  --executor-cores 1  target/scala-2.10/*.jar \
        "$training_input" \
        "$bayes_pz_output" \
        "$bayes_pwz_output" \
        || return  1
}
function bayes_predict() {
    bayes_pz_input=/gaoyunxiang/model_merge/bayes_pz_output
    bayes_pwz_input=/gaoyunxiang/model_merge/bayes_pwz_output
    test_input=/gaoyunxiang/model_merge/vdian_test_output
    alpha=0.00005
    beta=1
    $spark_path/spark-submit  --name "model bayes predict @_@gaoyunxiang" --class "BayesPredict" --master yarn-client  --num-executors 100  --driver-memory 60g  --executor-memory 20g  --executor-cores 2  target/scala-2.10/*.jar \
        "$alpha" \
        "$beta" \
        "$bayes_pz_input" \
        "$bayes_pwz_input" \
        "$test_input" \
        || return  1
}
$@
