package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @Author: magicyoung
 * @Date: 2019/5/21 14:39
 * @Description: Spark SQL 读取txt文件
 */
public class SQLTest01 {
    /**
     * 1. Person 自定义类必须用 public 修饰
     * 2. 自定义类需要实现序列化接口
     * 3. RDD 转换为 DataFrame 字段按照 asicc 排序
     * @param args
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SQLTest01");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> rdd1 = sc.textFile("F:\\person.txt");
        JavaRDD<Person> personJavaRDD = rdd1.map(new Function<String, Person>() {
            public Person call(String line) throws Exception {
                Person p = new Person();
                String[] split = line.split(",");
                p.setId(Integer.parseInt(split[0]));
                p.setName(split[1]);
                p.setAge(Integer.parseInt(split[2]));
                return p;
            }
        });

        Dataset<Row> df = sqlContext.createDataFrame(personJavaRDD, Person.class);
        df.show();

        sc.stop();
    }
}
