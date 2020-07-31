package ecnu.db.constraintchain.chain;

import ecnu.db.constraintchain.filter.logical.AndNode;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFilterNode extends ConstraintChainNode {
    private AndNode root;
    private BigDecimal probability;

    public ConstraintChainFilterNode(String tableName, BigDecimal probability, AndNode root) {
        super(tableName, ConstraintChainNodeType.FILTER);
        this.probability = probability;
        this.root = root;
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
