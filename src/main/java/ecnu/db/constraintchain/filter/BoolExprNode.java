package ecnu.db.constraintchain.filter;

import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.exception.TouchstoneToolChainException;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author wangqingshuai
 * todo 当前认为所有的BoolExprNode都是相互独立的
 */
public interface BoolExprNode {
    /**
     * 计算所有子节点的概率
     *
     * @param probability 当前节点的总概率
     * @throws TouchstoneToolChainException isnull算子和operation互斥，会导致概率为0，输入异常
     */
    List<AbstractFilterOperation> calculateProbability(BigDecimal probability) throws TouchstoneToolChainException;

    /**
     * 获得当前布尔表达式节点的类型
     * @return 类型
     */
    BoolExprType getType();
}
