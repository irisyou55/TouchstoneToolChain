package ecnu.db.schema.column;


/**
 * @author qingshuai.wang
 */
public abstract class AbstractColumn {
    private final ColumnType columnType;
    protected float nullPercentage;
    protected final String columnName;

    public AbstractColumn(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    /**
     * 获取该列非重复值的个数
     *
     * @return 非重复值的个数
     */
    public abstract int getNdv();

    public String getColumnName() {
        return columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }
}
