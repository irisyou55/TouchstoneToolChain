package ecnu.db.constraintchain.chain;

import ecnu.db.constraintchain.filter.logical.AndNode;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFilterNode extends ConstraintChainNode {
    private AndNode root;
    private BigDecimal probability;
    private AbstractFilterOperation[] abstractFilterOperations;

    public ConstraintChainFilterNode(String tableName, BigDecimal probability, AndNode root) {
        super(tableName, ConstraintChainNodeType.FILTER);
        this.probability = probability;
        this.root = root;
    }

    public ConstraintChainFilterNode(String tableName, String constraintChainInfo) {
        super(tableName, ConstraintChainNodeType.FILTER);
        //todo 解析constraintChainInfo
        // 如果是一元的FilterOperation 构造为UnitFilterOperation
        // 如果是多元的FilterOperation 构造为MultipleFilterOperation
        // 如果存在and和or需要使用logicalNode构建逻辑树，计算每个FilterOperation的概率
    }

    @Override
    public String toString() {
        return String.format("{root:%s,probability:%s}", root.toString(), probability);
    }
}
