package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.AbstractColumn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author alan
 */
public class BetweenFilterOperation extends UniVarFilterOperation {
    private List<Parameter> lessParameters = new ArrayList<>();
    private CompareOperator lessOperator;
    private List<Parameter> greaterParameters = new ArrayList<>();
    private CompareOperator greaterOperator;
    public BetweenFilterOperation(String columnName) {
        super(columnName, CompareOperator.BETWEEN);
    }

    @Override
    public void instantiateParameter(List<AbstractColumn> columns) {

    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    @Override
    public List<Parameter> getParameters() {
        List<Parameter> parameters = new ArrayList<>(lessParameters);
        parameters.addAll(greaterParameters);
        return parameters;
    }

    public void addLessParameters(Collection<Parameter> parameter) {
        lessParameters.addAll(parameter);
    }

    public void addGreaterParameters(Collection<Parameter> parameter) {
        greaterParameters.addAll(parameter);
    }

    public void setLessOperator(CompareOperator lessOperator) {
        this.lessOperator = lessOperator;
    }

    public void setGreaterOperator(CompareOperator greaterOperator) {
        this.greaterOperator = greaterOperator;
    }
}
