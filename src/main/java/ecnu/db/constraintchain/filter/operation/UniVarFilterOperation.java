package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.column.AbstractColumn;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.*;

/**
 * @author wangqingshuai
 */
public class UniVarFilterOperation extends AbstractFilterOperation {
    private String columnName;
    private Boolean hasNot = false;
    private CompareOperator leftOperator;
    private List<Parameter> leftParameters;
    private CompareOperator rightOperator;
    private List<Parameter> rightParameters;

    public UniVarFilterOperation(String columnName, CompareOperator operator) throws TouchstoneToolChainException {
        super(operator);
        this.columnName = columnName;
        // 小于EQ的只有GE GT LE LT，为between的tag
        if (operator.ordinal() < CompareOperator.EQ.ordinal()) {
            CompareOperator normalOperator = operator;
            if (hasNot) {
                switch (operator) {
                    case LE:
                        normalOperator = GT;
                        break;
                    case LT:
                        normalOperator = GE;
                        break;
                    case GE:
                        normalOperator = LT;
                        break;
                    case GT:
                        normalOperator = LE;
                        break;
                    default:
                        throw new TouchstoneToolChainException("不属于between的标志");
                }
            }
            if (normalOperator.ordinal() > GT.ordinal()) {
                rightOperator = normalOperator;
                rightParameters = parameters;
            } else {
                leftOperator = normalOperator;
                leftParameters = parameters;
            }
        }
    }


    /**
     * 当只有一个operator时，返回非null的operator，当存在两个时，返回operator
     *
     * @return 第一个合法的operator
     */
    @Override
    public CompareOperator getOperator() {
        if (operator.ordinal() < EQ.ordinal()) {
            return leftOperator != null ? leftOperator : rightOperator;
        } else {
            return operator;
        }
    }

    @Override
    public List<Parameter> getParameters() {
        if (operator.ordinal() < EQ.ordinal()) {
            List<Parameter> parameters = new LinkedList<>();
            parameters.addAll(leftParameters);
            parameters.addAll(rightParameters);
            return parameters;
        } else {
            return parameters;
        }
    }

    @Override
    public void addParameter(Parameter parameter) {
        if (operator.ordinal() < EQ.ordinal()) {
            if (rightOperator == null) {
                leftParameters.add(parameter);
            } else {
                rightParameters.add(parameter);
            }
        } else {
            this.parameters.add(parameter);
        }
    }

    @Override
    public void setParameters(List<Parameter> parameters) {
        if (operator.ordinal() < EQ.ordinal()) {
            if (rightOperator == null) {
                this.parameters.addAll(parameters);
            } else {
                this.rightParameters.addAll(parameters);
            }
        } else {
            this.parameters.addAll(parameters);
        }
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
        if (uniVarFilterOperationOperator.ordinal() > CompareOperator.GT.ordinal()) {
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
            if (leftOperator == null) {
                leftOperator = uniVarFilterOperationOperator;
                leftParameters = uniVarFilterOperation.getParameters();
            } else if (leftOperator.ordinal() < uniVarFilterOperationOperator.ordinal()) {
                leftOperator = uniVarFilterOperationOperator;
                leftParameters.addAll(uniVarFilterOperation.getParameters());
            } else {
                leftParameters.addAll(uniVarFilterOperation.getParameters());
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
