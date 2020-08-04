package ecnu.db.constraintchain.filter.logical;

import ch.obermuhlner.math.big.BigDecimalMath;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.util.List;

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
    public List<AbstractFilterOperation> calculateProbability(BigDecimal probability) throws TouchstoneToolChainException {
        BigDecimal nodeProbability = new BigDecimal(probability.toString());
        nodeProbability = BigDecimal.ONE.subtract(BigDecimalMath.sqrt(BigDecimal.ONE.subtract(nodeProbability), CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION));
        List<AbstractFilterOperation> operations = leftNode.calculateProbability(nodeProbability);
        operations.addAll(rightNode.calculateProbability(nodeProbability));
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
