package org.nilanjan;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

public class GetDataFromHttpEndpoint {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("GetDataFromHttpEndpoint").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        String endpointUrl = "https://jsonplaceholder.typicode.com/posts";
        JavaReceiverInputDStream<String> lines = jssc.receiverStream(new HttpReceiver(endpointUrl));
        lines.foreachRDD(rdd -> {
            rdd.foreach(data -> {
                System.out.println(data);
            });
        });
        jssc.start();
        jssc.awaitTermination();
    }
}
