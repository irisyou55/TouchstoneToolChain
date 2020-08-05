package ecnu.db.constraintchain.filter.logical;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.operation.*;
import ecnu.db.exception.CalculateProbabilityException;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.constraintchain.filter.BoolExprType.*;
import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;

/**
 * @author wangqingshuai
 */
public class AndNode implements BoolExprNode {
    private BoolExprType type = BoolExprType.AND;
    private LinkedList<BoolExprNode> children;

    public AndNode() {
        this.children = new LinkedList<>();
    }

    public void addChild(BoolExprNode logicalNode) {
        children.add(logicalNode);
    }

    /**
     * todo 当前And计算概率时，未考虑单值算子和多值算子的相互影响
     * <p>
     * todo 当前And计算概率时，假设可以合并的operation在同一组children中
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
        for (BoolExprNode child : children) {
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
                    if (!hasNot && !probability.equals(BigDecimal.ZERO)) {
                        throw new CalculateProbabilityException(String.format("and中包含了isnull(%s)与其他运算, 冲突而总概率不为0", ((IsNullFilterOperation) child).getColumnName()));
                    }
                } else {
                    BigDecimal nullProbability = ((IsNullFilterOperation) child).getProbability();
                    BigDecimal toDivide = hasNot ? BigDecimal.ONE.subtract(nullProbability) : nullProbability;
                    if (toDivide.equals(BigDecimal.ZERO)) {
                        if (!probability.equals(BigDecimal.ZERO)) {
                            throw new CalculateProbabilityException(String.format("'%s'的概率为0而and总概率不为0", child.toString()));
                        }
                    } else {
                        probability = probability.divide(toDivide, BIG_DECIMAL_DEFAULT_PRECISION);
                    }
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        UniVarFilterOperation.merge(otherNodes, col2uniFilters);

        probability = BigDecimalMath.pow(probability, BigDecimal.ONE.divide(BigDecimal.valueOf(otherNodes.size()), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION);

        List<AbstractFilterOperation> operations = new LinkedList<>();
        for (BoolExprNode node : otherNodes) {
            operations.addAll(node.pushDownProbability(probability, columns));
        }

        return operations;
    }

    @Override
    public BoolExprType getType() {
        return type;
    }

    public void setType(BoolExprType type) {
        this.type = type;
    }

    public LinkedList<BoolExprNode> getChildren() {
        return children;
    }

    public void setChildren(LinkedList<BoolExprNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return String.format("and(%s)", children.stream().map(BoolExprNode::toString).collect(Collectors.joining(", ")));
    }
}
