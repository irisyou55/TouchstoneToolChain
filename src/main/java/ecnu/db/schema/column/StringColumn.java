package ecnu.db.schema.column;

import java.math.BigDecimal;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author qingshuai.wang
 */
public class StringColumn extends AbstractColumn {
    private int minLength;
    private int maxLength;
    private int ndv;

    public StringColumn() {
        super(null, ColumnType.VARCHAR);
    }

    public StringColumn(String columnName) {
        super(columnName, ColumnType.VARCHAR);
    }

    public int getMinLength() {
        return minLength;
    }

    public void setMinLength(int minLength) {
        this.minLength = minLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    @Override
    public int getNdv() {
        return this.ndv;
    }

    @Override
    protected String generateRandomData(BigDecimal minProbability, BigDecimal maxProbability) {
        throw new UnsupportedOperationException();
    }

    public String generateLikeData(String likeStr) {
        String prefix = "", postfix = "";
        if (likeStr.startsWith("%")) {
            prefix = "%";
        }
        if (likeStr.endsWith("%")) {
            postfix = "%";
        }
        byte[] array = new byte[new Random().nextInt(maxLength - minLength) + minLength];
        new Random().nextBytes(array);
        return String.format("%s%s%s", prefix, new String(array, UTF_8), postfix);
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }
}
