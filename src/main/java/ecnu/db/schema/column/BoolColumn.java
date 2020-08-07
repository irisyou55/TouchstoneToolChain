package ecnu.db.schema.column;

import java.math.BigDecimal;

/**
 * @author qingshuai.wang
 */
public class BoolColumn extends AbstractColumn {
    private BigDecimal trueProbability;

    public BoolColumn() {
        super(null, ColumnType.BOOL);
    }

    public BoolColumn(String columnName) {
        super(columnName, ColumnType.BOOL);
    }

    @Override
    public int getNdv() {
        return -1;
    }
}
