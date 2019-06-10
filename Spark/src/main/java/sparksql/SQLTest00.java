package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @Author: magicyoung
 * @Date: 2019/5/21 10:56
 * @Description:
 */
public class SQLTest00 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SQLTest00");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> df = sqlContext.read().format("json").load("F:\\json");
        df.show();

        df.printSchema();

        Dataset<Row> df1 = df.select(df.col("name"), df.col("age")).where(df.col("age").gt(18));
        df1.show();

        df.registerTempTable("t1");
        Dataset<Row> df2 = sqlContext.sql("select * from t1 where age >= 18");
        df2.show();

        JavaRDD<Row> rowJavaRDD = df2.javaRDD();
        sc.stop();
    }
}
