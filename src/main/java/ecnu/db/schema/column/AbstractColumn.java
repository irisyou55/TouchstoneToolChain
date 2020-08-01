package ecnu.db.schema.column;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

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

    @JsonGetter
    @SuppressWarnings("unused")
    public float getNullPercentage() {
        return nullPercentage;
    }

    public String getColumnName() {
        return columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    @JsonSetter
    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }
}
