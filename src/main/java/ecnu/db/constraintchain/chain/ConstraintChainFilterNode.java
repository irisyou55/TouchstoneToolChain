package ecnu.db.constraintchain.chain;

import ecnu.db.constraintchain.filter.logical.AndNode;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.exception.TouchstoneToolChainException;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFilterNode extends ConstraintChainNode {
    private AndNode root;
    private BigDecimal probability;
    // 概率下推后，该节点的所有operation
    private List<AbstractFilterOperation> operations;

    public List<AbstractFilterOperation> getOperations() {
        return operations;
    }

    public ConstraintChainFilterNode(String tableName, BigDecimal probability, AndNode root) throws TouchstoneToolChainException {
        super(tableName, ConstraintChainNodeType.FILTER);
        this.probability = probability;
        this.root = root;
        operations = root.calculateProbability(probability);
    }

    public void setRoot(AndNode root) {
        this.root = root;
    }

    public AndNode getRoot() {
        return root;
    }

    public void setProbability(BigDecimal probability) {
        this.probability = probability;
    }

    public BigDecimal getProbability() {
        return probability;
    }

    @Override
    public String toString() {
        return String.format("{root:%s,probability:%s}", root.toString(), probability);
    }
}
