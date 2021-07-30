import com.googlecode.ipv6.IPv6Address;
import static org.apache.spark.sql.functions.udf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.Objects;

public class IcebergSparkUtil {

    public static byte[] ipv6ToBytes(String ipv6Str) {
        return IPv6Address.fromString(ipv6Str).toByteArray();
    }

    public static UserDefinedFunction ipv6ToBytesUdf()  {
        return udf((UDF1<String, byte[]>) IcebergSparkUtil::ipv6ToBytes, DataTypes.BinaryType);
    }

    public static void register(SparkSession spark) {
        spark.udf().register("ipv6_to_bytes", ipv6ToBytesUdf());
    }


    private static SparkSession createContext() {
        return SparkSession.builder().master("local")
                .appName("iceberg.test")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", "warehouse")
                .config("spark.sql.warehouse.dir", "warehouse")
                .getOrCreate();

    }


    public static void createTable() {
        SparkSession spark = createContext();
        UserDefinedFunction ipv6UDF = ipv6ToBytesUdf();
        Dataset<Row> df = spark.read().option("header", true)
                .csv("src/test/resources/ipv6_addresses_header.txt")
                .withColumn("ip_bytes", ipv6UDF.apply(col("ip_string")));
        df.printSchema();
        spark.sql(String.format("CREATE TABLE local.ipv6 (%s)", df.schema().toDDL()));

    }


}
