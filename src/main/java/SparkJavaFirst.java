

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.mapper.JavaBeanColumnMapper;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SparkJavaFirst implements Serializable {

    static org.slf4j.Logger logger = LoggerFactory.getLogger(SparkJavaFirst.class);
    public static void main(String args[]){

        SparkJavaFirst sjf = new SparkJavaFirst();
        sjf.sparkCalc();
        /*SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkJavaFirst");
        JavaSparkContext sc = new JavaSparkContext(conf);*

        JavaRDD<String> input = sc.textFile("G:\\WordCount.txt");
        logger.info("Started reading file");


        *//*JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {

            @SuppressWarnings("unchecked")
            public Iterator<String> call(String s) throws Exception {
                // TODO Auto-generated method stub
                return (Iterator<String>) Arrays.asList(s.split(","));
            }
        });

        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String s) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer x, Integer y) throws Exception {
                // TODO Auto-generated method stub
                return x+y;
            }
        });

        reducedCounts.saveAsTextFile("G:\\output.txt");
*/
    }

    public void sparkCalc() {

        List<Product> products = Arrays.asList(
                new Product(0, "All products", Collections.<Integer>emptyList()),
                new Product(1, "Product A", Arrays.asList(0)),
                new Product(4, "Product A1", Arrays.asList(0, 1)),
                new Product(5, "Product A2", Arrays.asList(0, 1)),
                new Product(2, "Product B", Arrays.asList(0)),
                new Product(6, "Product B1", Arrays.asList(0, 2)),
                new Product(7, "Product B2", Arrays.asList(0, 2)),
                new Product(3, "Product C", Arrays.asList(0)),
                new Product(8, "Product C1", Arrays.asList(0, 3)),
                new Product(9, "Product C2", Arrays.asList(0, 3))
        );
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkJavaFirst")
                .set("spark.cassandra.connection.host", "127.0.0.1").
                        set("spark.cassandra.input.consistency.level", "ONE").
                        set("spark.cassandra.read.timeout_ms","360000");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

/*
        JavaRDD<Product> productJavaRDD = sc.parallelize(products);
        JavaPairRDD<String, Integer> pairRDD = productJavaRDD.mapToPair(new PairFunction<Product, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Product product) throws Exception {
                return new Tuple2<String, Integer>(product.getName(),1);
            }
        });

        System.out.println(pairRDD.take(6));*/


//Normal Row
        JavaRDD<CassandraRow> cassandraJavaRDD =  CassandraJavaUtil.javaFunctions(sc).
                cassandraTable("people","people" ).where("salary > 1000").
                select("last_name","salary").filter(row -> row.getString("last_name").equals("mehul"));
        
// TO Tuple
        JavaRDD<Tuple2<String, Double>> javaRDD =  CassandraJavaUtil.javaFunctions(sc).
                cassandraTable("people","people",CassandraJavaUtil.mapRowToTuple(String.class,Double.class) ).where("salary > 1000").
                select("last_name","salary");
// saving tuple to database.
        CassandraJavaUtil.javaFunctions(javaRDD).
                writerBuilder("people", "filterpeople",
                        CassandraJavaUtil.mapTupleToRow(String.class,Double.class)).
                withConsistencyLevel(ConsistencyLevel.ONE).
                withColumnSelector(CassandraJavaUtil.someColumns("last_name","salary")).
                saveToCassandra();

        System.out.println("Output is"+cassandraJavaRDD.take(10).toString());
        System.out.println("Output is"+javaRDD.take(10).toString());

    }

}
