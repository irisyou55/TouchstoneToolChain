package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.AbstractColumn;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author wangqingshuai
 */
public abstract class AbstractFilterOperation implements BoolExprNode {
    /**
     * 此filter包含的参数
     */
    protected List<Parameter> parameters = new ArrayList<>();
    /**
     * 此filter operation的操作符
     */
    protected CompareOperator operator;
    /**
     * 此filter operation的过滤比
     */
    protected BigDecimal probability;

    /**
     * 计算Filter Operation实例化的参数
     */
    public abstract void instantiateParameter(List<AbstractColumn> columns);

    @Override
    public List<AbstractFilterOperation> calculateProbability(BigDecimal probability) {
        this.probability = probability;
        return Collections.singletonList(this);
    }

    public AbstractFilterOperation(CompareOperator operator) {
        this.operator = operator;
    }

    public void addParameter(Parameter parameter) {
        parameters.add(parameter);
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public List<Parameter> getParameters() {
        return this.parameters;
    }

    public CompareOperator getOperator() {
        return operator;
    }

    public void setOperator(CompareOperator operator) {
        this.operator = operator;
    }

    public void setProbability(BigDecimal probability) {
        this.probability = probability;
    }

    public BigDecimal getProbability() {
        return probability;
    }
}
