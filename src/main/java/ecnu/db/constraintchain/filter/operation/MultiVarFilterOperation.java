package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.constraintchain.arithmetic.value.ColumnNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.AbstractColumn;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wangqingshuai
 */
public class MultiVarFilterOperation extends AbstractFilterOperation {
    private ArithmeticNode arithmeticTree;

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
            colNames.add(((ColumnNode) node).getColumnName());
        }
        getColNames(node.getLeftNode(), colNames);
        getColNames(node.getRightNode(), colNames);
    }

    /**
     * todo 通过计算树计算概率，暂时不考虑其他FilterOperation对于此操作的阈值影响
     */
    @Override
    public void instantiateParameter(List<AbstractColumn> columns) {
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
}
