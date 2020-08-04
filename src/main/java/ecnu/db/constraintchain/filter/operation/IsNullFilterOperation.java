package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.schema.column.AbstractColumn;

import java.util.List;

/**
 * @author alan
 */
public class IsNullFilterOperation extends AbstractFilterOperation {
    private String columnName;
    private Boolean hasNot = false;
    private float nullProbability;

    public float getNullProbability() {
        return nullProbability;
    }

    public void setNullProbability(float nullProbability) {
        this.nullProbability = nullProbability;
    }

    public IsNullFilterOperation(String columnName, float nullProbability) {
        super(CompareOperator.ISNULL);
        this.columnName = columnName;
        this.nullProbability = nullProbability;
    }

    @Override
    public void instantiateParameter(List<AbstractColumn> columns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.ISNULL_FILTER_OPERATION;
    }

    @Override
    public String toString() {
        if (hasNot) {
            return String.format("not(isnull(%s))", this.columnName);
        }
        return String.format("isnull(%s)", this.columnName);
    }

    public Boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(Boolean hasNot) {
        this.hasNot = hasNot;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

}
