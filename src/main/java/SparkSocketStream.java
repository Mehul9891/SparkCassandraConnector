import groovy.util.logging.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

@Slf4j(value = "info")
public class SparkSocketStream {


     public static void main(String [] args){

            SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafkaStream");

            JavaSparkContext jsc = new JavaSparkContext(conf);
            JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(10));

         JavaReceiverInputDStream<String> stream = ssc.socketTextStream("192.168.43.6", 9999
         , StorageLevel.MEMORY_AND_DISK_SER());
            JavaPairDStream<String, Integer> output= stream.
                    flatMap( line -> Arrays.asList(line.split(" ")).iterator()).
                    mapToPair(record -> new Tuple2<String, Integer>(record , 1)).
                    reduceByKey((x,y) -> (x+y));
            System.out.println("Started SPARK Streaming");
            System.out.println("count of the rdd is :"+output.count());
            output.print();
            ssc.start();
            try {
                   ssc.awaitTerminationOrTimeout(100000);
            } catch (InterruptedException e) {
                   e.printStackTrace();
            }
/*
            JavaDStream<String> stream = ssc.textFileStream("G:\\WordCount.txt").
                    flatMap(line -> (Iterator<String>)Arrays.asList(line.split(",")));
            JavaPairDStream<String, Integer> output= stream.
            mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2(s,1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer x, Integer y) throws Exception {
                    return x+y;
                }
            });*/


        }
}
