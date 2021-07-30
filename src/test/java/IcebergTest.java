import org.junit.jupiter.api.Test;

public class IcebergTest {

    @Test
    void test() {
        IcebergSparkUtil.createTable();
        IcebergSparkUtil.loadTable();
    }
}
