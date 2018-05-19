import groovy.util.logging.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import scala.Tuple2;

import java.util.*;

@Slf4j(value = "info")
public class SparkKafkaStream{


     public static void main(String [] args){

            SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkKafkaStream");
                   /* .set("spark.cassandra.connection.host", "127.0.0.1").
                            set("spark.cassandra.input.consistency.level", "ONE").
                            set("spark.ca   ssandra.read.timeout_ms","360000");*/

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", "localhost:9094");
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "test");
            kafkaParams.put("auto.commit.interval.ms", "1000");
            kafkaParams.put("session.timeout.ms", "30000");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", true);
            Collection<String> topics = Arrays.asList("my_latest_topic");
            JavaSparkContext jsc = new JavaSparkContext(conf);
            JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(2));

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    ssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaParams)
            );
            JavaPairDStream<String, Integer> output= stream.
                    flatMap( line -> (Iterator<String>)Arrays.asList(line.value().split(" "))).
                    mapToPair(record -> new Tuple2<String, Integer>(record , 1)).
                    reduceByKey((x,y) -> (x+y));
            System.out.println("Started SPARK Streaming");
            System.out.println("count of the rdd is :"+output.count());
            output.print();
            ssc.start();
            try {
                   ssc.awaitTerminationOrTimeout(10000);
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
