package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.AbstractColumn;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wangqingshuai
 */
public class UniVarFilterOperation extends AbstractFilterOperation {
    private String columnName;
    private Boolean hasNot = false;
    private CompareOperator rightOperator;
    private List<Parameter> rightParameters;

    public UniVarFilterOperation(String columnName, CompareOperator operator) {
        super(operator);
        this.columnName = columnName;
        if (operator.ordinal() > CompareOperator.EQ.ordinal()) {
            rightOperator = operator;
            rightParameters = parameters;
            parameters = null;
            this.operator = null;
        }
    }


    @Override
    public CompareOperator getOperator() {
        return rightOperator == null ? operator : rightOperator;
    }

    @Override
    public List<Parameter> getParameters() {
        List<Parameter> parameters = new LinkedList<>();
        parameters.addAll(this.parameters);
        parameters.addAll(rightParameters);
        return parameters;
    }

    @Override
    public String toString() {
        List<String> content = parameters.stream().map(Parameter::toString).collect(Collectors.toList());
        content.add(0, String.format("%s", columnName));
        if (hasNot) {
            return String.format("not(%s(%s))", operator.toString().toLowerCase(), String.join(", ", content));
        }
        return String.format("%s(%s)", operator.toString().toLowerCase(), String.join(", ", content));
    }

    /**
     * merge一个operation
     *
     * @param uniVarFilterOperation 待merge的operation
     */
    public void merge(UniVarFilterOperation uniVarFilterOperation) {
        CompareOperator uniVarFilterOperationOperator = uniVarFilterOperation.getOperator();
        if (uniVarFilterOperationOperator.ordinal() > CompareOperator.EQ.ordinal()) {
            if (rightOperator == null) {
                rightOperator = uniVarFilterOperationOperator;
                rightParameters = uniVarFilterOperation.getParameters();
            } else if (rightOperator.ordinal() < uniVarFilterOperationOperator.ordinal()) {
                rightOperator = uniVarFilterOperationOperator;
                rightParameters.addAll(uniVarFilterOperation.getParameters());
            } else {
                rightParameters.addAll(uniVarFilterOperation.getParameters());
            }
        } else {
            if (operator == null) {
                operator = uniVarFilterOperationOperator;
                parameters = uniVarFilterOperation.getParameters();
            } else if (operator.ordinal() < uniVarFilterOperationOperator.ordinal()) {
                operator = uniVarFilterOperationOperator;
                parameters.addAll(uniVarFilterOperation.getParameters());
            } else {
                parameters.addAll(uniVarFilterOperation.getParameters());
            }
        }
    }

    /**
     * todo 参数实例化
     */
    @Override
    public void instantiateParameter(List<AbstractColumn> columns) {

    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    public Boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(Boolean hasNot) {
        this.hasNot = hasNot;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
