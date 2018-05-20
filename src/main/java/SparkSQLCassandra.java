
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import java.util.HashMap;

public class SparkSQLCassandra {
/*
    key | last_name | salary
-----+-----------+------------
        5 |     mohit | 9.0286e+09
            10 |     ankit | 9.0286e+09
            13 |    govind | 9.0286e+09
            11 |    mahesh | 9.0286e+09
            1 |     mehul | 9.0286e+09
            8 |    poonam | 9.0286e+09
            2 |      amit | 9.0286e+09
            4 |     manoj | 9.0286e+09
            15 |    rakesh | 9.0286e+09
            7 |     rohit | 9.0286e+09
            6 |     rahul | 9.0286e+09
            9 |    ankita | 9.0286e+09
            14 |    ramesh | 9.0286e+09
            12 |     gopal | 9.0286e+09
            3 |     rahul | 9.0286e+09*/
    public static void main (String args[]){

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLCassandra")
                .set("spark.cassandra.connection.host", "127.0.0.1").
                        set("spark.cassandra.input.consistency.level", "ONE").
                        set("spark.cassandra.read.timeout_ms","360000");
        JavaSparkContext jsc = new JavaSparkContext(conf);
         SQLContext sqlContext = new SQLContext(jsc.toSparkContext(jsc));

        Dataset<Row> reader = sqlContext.read().format("org.apache.spark.sql.cassandra")
                .options(new HashMap<String,String>() {
                    {
                        put("keyspace", "people");
                        put("table", "people");
                    }
                }).load();

        reader.show();
        sqlContext.registerDataFrameAsTable(reader,"Employees");
        sqlContext.sql("SELECT * from Employees where last_name = 'mehul'").show();

    }
}
