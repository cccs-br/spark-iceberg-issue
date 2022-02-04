import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IcebergTests {

    @BeforeAll
    static void setup() {
        if(!Files.exists(Paths.get("warehouse"))) {
            IcebergTestUtil.createTable();
            IcebergTestUtil.loadTable();
        }
    }

    @Test
    void query_all() {
        SparkSession spark = IcebergTestUtil.createSession();
        Dataset<Row> df = spark.sql("select * from iceberg.db.ipv6");
        assertEquals(102, df.count() );
    }

    @Test
    void filter_spark_c1_ok() {
        SparkSession spark = IcebergTestUtil.createSession();
        Dataset<Row> df = spark.read().parquet("warehouse/db/ipv6/data").where(
                "ip_bytes > ipv6_to_bytes('6540:cf5b:fed2:100e:71f1:ae06:76d4:c2f')"
        );
        assertEquals(59, df.count());
    }

    @Test
    void filter_spark_c2_ok() {
        SparkSession spark = IcebergTestUtil.createSession();
        byte[] crit = IcebergTestUtil.ipv6ToBytes("6540:cf5b:fed2:100e:71f1:ae06:76d4:c2f");
        Dataset<Row> df = spark.read().parquet("warehouse/db/ipv6/data").where(
                col("ip_bytes").$greater(crit)
        );
        assertEquals(59, df.count());
    }

    @Test
    void filter_iceberg_c1_ok() {
        SparkSession spark = IcebergTestUtil.createSession();
        Dataset<Row> df = spark.sql("select * from iceberg.db.ipv6").where(
                "ip_bytes > ipv6_to_bytes('6540:cf5b:fed2:100e:71f1:ae06:76d4:c2f')"
        );
        assertEquals(59, df.count());
    }

    @Test
    void filter_iceberg_c2_error() {
        SparkSession spark = IcebergTestUtil.createSession();
        byte[] crit = IcebergTestUtil.ipv6ToBytes("6540:cf5b:fed2:100e:71f1:ae06:76d4:c2f");
        Dataset<Row> df = spark.sql("select * from iceberg.db.ipv6").where(
                col("ip_bytes").$greater(crit)
        );
        assertEquals(59, df.count());
    }

    @Test
    void filter_iceberg_c2_error_with_lit() {
        SparkSession spark = IcebergTestUtil.createSession();
        Column crit = lit(IcebergTestUtil.ipv6ToBytes("6540:cf5b:fed2:100e:71f1:ae06:76d4:c2f"));
        Dataset<Row> df = spark.sql("select * from iceberg.db.ipv6").where(
                col("ip_bytes").$greater(crit)
        );
        assertEquals(59, df.count());
    }

}


