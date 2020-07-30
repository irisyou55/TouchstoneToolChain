package ecnu.db.constraintchain.chain;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class ConstraintChainPkJoinNode extends ConstraintChainNode {
    private String[] pkColumns;
    private int pkTag;
    private BigDecimal probability;

    public ConstraintChainPkJoinNode(String tableName, int pkTag, String[] pkColumns, BigDecimal probability) {
        super(tableName, ConstraintChainNodeType.PK_JOIN);
        this.pkTag = pkTag;
        this.pkColumns = pkColumns;
        this.probability = probability;
    }

    /**
     * 构建ConstraintChainPkJoinNode对象
     *
     * @param constraintChainInfo 获取到的约束链信息
     */
    public ConstraintChainPkJoinNode(String tableName, String constraintChainInfo) {
        super(tableName, ConstraintChainNodeType.PK_JOIN);
        //todo 解析constraintChainInfo
    }

    @Override
    public String toString() {
        return String.format("{pkTag:%d,probability:%s,pkColumns:%s}", pkTag, probability, Arrays.toString(pkColumns));
    }
}
