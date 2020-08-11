package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.constraintchain.arithmetic.value.ColumnNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author wangqingshuai
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MultiVarFilterOperation extends AbstractFilterOperation {
    private ArithmeticNode arithmeticTree;

    public MultiVarFilterOperation() {
        super(null);
    }

    public MultiVarFilterOperation(CompareOperator operator, ArithmeticNode arithmeticTree) {
        super(operator);
        this.arithmeticTree = arithmeticTree;
    }

    /**
     * 获取计算树访问的所有列名
     * @return 计算树访问的所有列名
     */
    public HashSet<String> getColNames() {
        HashSet<String> colNames = new HashSet<>();
        getColNames(arithmeticTree, colNames);
        return colNames;
    }

    private void getColNames(ArithmeticNode node, HashSet<String> colNames) {
        if (node == null) {
            return;
        }
        if (node.getType() == ArithmeticNodeType.COLUMN) {
            colNames.add(String.format("%s.%s", ((ColumnNode) node).getCanonicalTableName(), ((ColumnNode) node).getColumn().getColumnName()));
        }
        getColNames(node.getLeftNode(), colNames);
        getColNames(node.getRightNode(), colNames);
    }

    /**
     * todo 通过计算树计算概率，暂时不考虑其他FilterOperation对于此操作的阈值影响
     */
    @Override
    public void instantiateParameter(Map<String, AbstractColumn> columns) {
        if (operator.getType() == CompareOperator.TYPE.LESS || operator.getType() == CompareOperator.TYPE.GREATER) {
            float[] vector = arithmeticTree.getVector();
            int pos = probability.multiply(BigDecimal.valueOf(vector.length)).intValue();
            Arrays.sort(vector);
            parameters.forEach(param -> {
                if (CommonUtils.isInteger(param.getData())) {
                    param.setData(Integer.toString((int) vector[pos]));
                } else if (CommonUtils.isFloat(param.getData())) {
                    param.setData(Float.toString(vector[pos]));
                } else {
                    throw new UnsupportedOperationException();
                }
            });
        }
        else if (operator.getType() == CompareOperator.TYPE.EQUAL) {
            if (operator == CompareOperator.EQ) {

            }
            else if (operator == CompareOperator.NE) {

            }
            else {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.MULTI_FILTER_OPERATION;
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s)", operator.toString().toLowerCase(),
                arithmeticTree.toString(),
                parameters.stream().map(Parameter::toString).collect(Collectors.joining(", ")));
    }

    public void setArithmeticTree(ArithmeticNode arithmeticTree) {
        this.arithmeticTree = arithmeticTree;
    }

    public ArithmeticNode getArithmeticTree() {
        return arithmeticTree;
    }

    public void instantiateMultiVarParameter(Map<String, AbstractColumn> columns) {

    }
}
