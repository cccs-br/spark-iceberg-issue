import com.googlecode.ipv6.IPv6Address;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

public class IcebergTestUtil {

    public static byte[] ipv6ToBytes(String ipv6Str) {
        return IPv6Address.fromString(ipv6Str).toByteArray();
    }

    public static UserDefinedFunction ipv6ToBytesUdf()  {
        return udf((UDF1<String, byte[]>) IcebergTestUtil::ipv6ToBytes, DataTypes.BinaryType);
    }

    public static void register(SparkSession spark) {
        spark.udf().register("ipv6_to_bytes", ipv6ToBytesUdf());
    }

    public static SparkSession createSession() {
        SparkSession spark = SparkSession.builder().master("local")
                .appName("iceberg.test")
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type", "hadoop")
                .config("spark.sql.catalog.iceberg.warehouse", "warehouse")
                .config("spark.sql.warehouse.dir", "warehouse")
                .getOrCreate();
        register(spark);
        return spark;
    }

    public static void createTable() {
        SparkSession spark = createSession();
        UserDefinedFunction ipv6UDF = ipv6ToBytesUdf();
        Dataset<Row> df = spark.read().option("header", true)
                .csv("src/test/resources/ipv6_addresses_header.txt")
                .withColumn("ip_bytes", ipv6UDF.apply(col("ip_string")));
        df.printSchema();
        spark.sql(String.format("CREATE TABLE iceberg.db.ipv6 (%s) USING iceberg", df.schema().toDDL()));
    }

    public static void loadTable() {
        SparkSession spark = createSession();
        UserDefinedFunction ipv6UDF = ipv6ToBytesUdf();
        Dataset<Row> df = spark.read().option("header", true)
                .csv("src/test/resources/ipv6_addresses_header.txt")
                .withColumn("ip_bytes", ipv6UDF.apply(col("ip_string")));
        df.write().mode("overwrite").insertInto("iceberg.db.ipv6");
    }

    public static void queryTable() {
        SparkSession spark = createSession();
        spark.sql("select * from iceberg.db.ipv6").show();
    }

}
