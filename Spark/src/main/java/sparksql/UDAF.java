package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/5/21 19:49
 * @Description: 用户自定义聚合函数
 */
public class UDAF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("UDAF");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("zhangshan", "lisi", "wangwu", "zhangsan", "zhangsan", "wangwu"));
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

        sqlContext.udf().register("CountUDAF", new UserDefinedAggregateFunction() {
            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0, 0);
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                buffer.update(0, buffer.getInt(0) + 1);
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0));
            }

            @Override
            public StructType bufferSchema() {
                return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("buffer", DataTypes.IntegerType, true)));
            }

            @Override
            public Object evaluate(Row buffer) {
                return buffer.getInt(0);
            }

            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }

            @Override
            public StructType inputSchema() {
                return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("name", DataTypes.IntegerType, true)));
            }

            /**
             * 确保一致性
             * @return
             */
            @Override
            public boolean deterministic() {
                return true;
            }
        });

        sqlContext.sql("select name, CountUDAF(name) from user group by name").show();
        sc.stop();
    }
}
