package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @Author: magicyoung
 * @Date: 2019/5/22 9:45
 * @Description: 开窗函数
 */

/**
 * row_number() 开窗函数
 * 主要是按照某个字段分组，然后取另一字段的前几个的值，相当于分组取 topN
 * row_number() over (partition by xxx order by xxx desc) xxx
 * 注意：
 * 如果 SQL 中使用到了开窗函数，那么这个 SQL 语句必须使用 HiveContext 来执行，HiveContext 默认情况下在本地是无法创建的
 */
public class WindowFun {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WindowFun");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);

        hiveContext.sql("use spark");
        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales (riqi string, leibie string, jine Int) "
                + "row format delimited fields terminated by '\t'");
        hiveContext.sql("load data local inpath '/root/' into table sales");

        Dataset<Row> result = hiveContext.sql("select riqi, leibie, jine"
                + "from ("
                + "select riqi, leibie, jine,"
                + "row_number() over (partition by leibie order by jine desc) rank"
                + "from sales) t"
                + "where t.rank <= 3"
        );

        result.show(100);

        /**
         * 将结果保存到 Hive 中的 sales_result 表中
         */
        result.write().mode(SaveMode.Overwrite).saveAsTable("sales_result");
        sc.stop();
    }
}
