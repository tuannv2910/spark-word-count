package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("spark://quickstart.cloudera:7077")
                .setAppName("Spark-word-count");
        try (JavaSparkContext sc =  new JavaSparkContext(conf)) {
            JavaRDD<String> textFile = sc.textFile("/user/cloudera/tuannv/hello.txt");
            JavaPairRDD<String, Integer> result = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a,b) -> (a+b));
            result.saveAsTextFile("user/cloudera/tuannv/hello.txt");
        }
    }
}