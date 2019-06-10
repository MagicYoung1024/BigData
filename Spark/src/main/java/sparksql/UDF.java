package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/5/21 19:35
 * @Description: 用户自定义函数
 */
public class UDF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("UDF");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("zhangshan", "lisi", "wangwu"));
        JavaRDD<Row> rowRDD = parallelize.map(new Function<String, Row>() {
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });

        /**
         * 动态添加 Schema 方式加载 DF
         */
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);

        df.registerTempTable("user");

        /**
         * 根据 UDF 函数参数的个数来决定是实现哪一个UDF
         */
        sqlContext.udf().register("StrLen", new UDF1<String, Integer>() {
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);

        sqlContext.sql("select name, StrLen(name) as length from user").show();

        sc.stop();
    }
}
