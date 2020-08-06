package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.AbstractColumn;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author alan
 */
public class RangeFilterOperation extends UniVarFilterOperation {
    private List<Parameter> lessParameters = new ArrayList<>();
    private CompareOperator lessOperator;
    private List<Parameter> greaterParameters = new ArrayList<>();
    private CompareOperator greaterOperator;
    public RangeFilterOperation(String columnName) {
        super(columnName, CompareOperator.RANGE);
    }

    @Override
    public void instantiateParameter(List<AbstractColumn> columns) {
        throw new NotImplementedException();
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    @Override
    public void setParameters(List<Parameter> parameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addParameter(Parameter parameter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Parameter> getParameters() {
        List<Parameter> parameters = new ArrayList<>(lessParameters);
        parameters.addAll(greaterParameters);
        return parameters;
    }

    public List<Parameter> getLessParameters() {
        return lessParameters;
    }

    public void addLessParameters(Collection<Parameter> parameter) {
        lessParameters.addAll(parameter);
    }

    public List<Parameter> getGreaterParameters() {
        return greaterParameters;
    }

    public void addGreaterParameters(Collection<Parameter> parameter) {
        greaterParameters.addAll(parameter);
    }

    public CompareOperator getLessOperator() {
        return lessOperator;
    }

    public void setLessOperator(CompareOperator lessOperator) {
        this.lessOperator = lessOperator;
    }

    public CompareOperator getGreaterOperator() {
        return greaterOperator;
    }

    public void setGreaterOperator(CompareOperator greaterOperator) {
        this.greaterOperator = greaterOperator;
    }
}
