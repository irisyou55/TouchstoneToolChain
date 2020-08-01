package ecnu.db.constraintchain.chain;

/**
 * @author wangqingshuai
 */
public class ConstraintChainNode {
    protected final String tableName;
    protected final ConstraintChainNodeType constraintChainNodeType;

    public ConstraintChainNode(String tableName, ConstraintChainNodeType constraintChainNodeType) {
        this.tableName = tableName;
        this.constraintChainNodeType = constraintChainNodeType;
    }

    public ConstraintChainNodeType getConstraintChainNodeType() {
        return constraintChainNodeType;
    }
}
