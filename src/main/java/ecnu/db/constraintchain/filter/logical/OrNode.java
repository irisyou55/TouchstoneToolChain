package ecnu.db.constraintchain.filter.logical;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.constraintchain.filter.operation.IsNullFilterOperation;
import ecnu.db.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.exception.CalculateProbabilityException;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static ecnu.db.constraintchain.filter.BoolExprType.*;
import static ecnu.db.constraintchain.filter.operation.UniVarFilterOperation.merge;
import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;

/**
 * @author wangqingshuai
 */
public class OrNode implements BoolExprNode {
    private BoolExprNode leftNode;
    private BoolExprNode rightNode;
    private final BoolExprType type = BoolExprType.OR;

    public BoolExprNode getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(BoolExprNode leftNode) {
        this.leftNode = leftNode;
    }

    public BoolExprNode getRightNode() {
        return rightNode;
    }

    public void setRightNode(BoolExprNode rightNode) {
        this.rightNode = rightNode;
    }

    /**
     * todo 考虑算子之间的相互依赖
     * todo 考虑or算子中包含isnull算子
     * <p>
     * 算子的概率为P(A or B) = P(!(!A and !B)) = M
     * P(A) = P(B) = 1-Sqrt(1-M)
     *
     * @param probability 当前节点的总概率值
     */
    @Override
    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability, Set<String> columns) throws CalculateProbabilityException {
        List<BoolExprNode> otherNodes = new LinkedList<>();
        Multimap<String, UniVarFilterOperation> col2uniFilters = ArrayListMultimap.create();
        // 1. 分离各种node
        // 2. 合并单列的operation
        // 3. 记录operation访问的各个列名
        for (BoolExprNode child : Arrays.asList(leftNode, rightNode)) {
            if (child.getType() == AND || child.getType() == OR) {
                otherNodes.add(child);
            } else if (child.getType() == UNI_FILTER_OPERATION) {
                col2uniFilters.put(((UniVarFilterOperation) child).getColumnName(), (UniVarFilterOperation) child);
            } else if (child.getType() == MULTI_FILTER_OPERATION) {
                otherNodes.add(child);
            } else if (child.getType() == ISNULL_FILTER_OPERATION) {
                String columnName = ((IsNullFilterOperation) child).getColumnName();
                boolean hasNot = ((IsNullFilterOperation) child).getHasNot();
                if (columns.contains(columnName)) {
                    if (hasNot && !probability.equals(((IsNullFilterOperation) child).getProbability())) {
                        throw new CalculateProbabilityException("or中包含了not(isnull(%s))与其他运算, 总概率不等于not isnull的概率");
                    }
                } else {
                    BigDecimal nullProbability = ((IsNullFilterOperation) child).getProbability();
                    BigDecimal toDivide = BigDecimal.ONE.subtract(hasNot ? BigDecimal.ONE.subtract(nullProbability) : nullProbability);
                    if (toDivide.equals(BigDecimal.ZERO)) {
                        if (!probability.equals(BigDecimal.ONE)) {
                            throw new CalculateProbabilityException(String.format("'%s'的概率为1而总概率不为1", child.toString()));
                        }
                    } else {
                        probability = BigDecimal.ONE.subtract(probability.divide(toDivide, BIG_DECIMAL_DEFAULT_PRECISION));
                    }
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        merge(otherNodes, col2uniFilters);

        probability = BigDecimalMath.pow(probability, BigDecimal.ONE.divide(BigDecimal.valueOf(otherNodes.size()), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION);

        List<AbstractFilterOperation> operations = new LinkedList<>();
        for (BoolExprNode node : otherNodes) {
            operations.addAll(node.pushDownProbability(BigDecimal.ONE.subtract(probability), columns));
        }

        return operations;
    }

    @Override
    public BoolExprType getType() {
        return type;
    }

    @Override
    public String toString() {
        return String.format("or(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
