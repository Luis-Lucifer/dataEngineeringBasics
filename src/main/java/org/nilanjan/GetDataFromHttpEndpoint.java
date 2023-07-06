package org.nilanjan;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

import java.io.FileWriter;
import java.io.IOException;

public class GetDataFromHttpEndpoint {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("GetDataFromHttpEndpoint").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        String endpointUrl = "https://api.publicapis.org/entries";
        JavaReceiverInputDStream<String> lines = jssc.receiverStream(new HttpReceiver(endpointUrl));
//        lines.foreachRDD(rdd -> {
//            rdd.foreach(data -> {
//                System.out.println(data);
//            });
//        });

        lines.foreachRDD(rdd ->{
            rdd.foreachPartition(records -> {
                while (records.hasNext()){
                    String record = records.next();
                    System.out.println(record);
                    save(record);
                }
            });
        });

        jssc.start();
        jssc.awaitTermination();
    }

    public static void save(String record) throws IOException {
        String filepath = "D:/E_minds/MyProjectWork/GetDataFrom_Http_endpoint/getDataFromURL/file.txt";

        FileWriter writer = new FileWriter(filepath,true);
        writer.write(record);
        writer.write(System.lineSeparator());
        writer.flush();
    }
}
