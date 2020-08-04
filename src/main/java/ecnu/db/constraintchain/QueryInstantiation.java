package ecnu.db.constraintchain;

import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.constraintchain.chain.ConstraintChainNode;
import ecnu.db.constraintchain.chain.ConstraintChainNodeType;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.exception.CannotFindColumnException;
import ecnu.db.schema.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author wangqingshuai
 */
public class QueryInstantiation {
    /**
     * todo 参数实例化计算
     */
    public static void compute(List<ConstraintChain> constraintChains, Map<String, Schema> schemas) throws CannotFindColumnException {
        //todo 1. 对于数值型的filter, 首先计算单元的filter, 然后计算多值的filter，
        //        对于bet操作，先记录阈值，然后选择合适的区间插入，等值约束也需选择合适的区间
        //        每个filter operation内部保存自己实例化后的结果
        //     2. 对于字符型的filter, 只有like和eq的运算，直接计算即可
        for (ConstraintChain constraintChain : constraintChains) {
            Schema schema = schemas.get(constraintChain.getTableName());
            for (ConstraintChainNode node : constraintChain.getNodes()) {
                if (node.getConstraintChainNodeType() == ConstraintChainNodeType.FILTER) {
                    List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).getOperations();
                    for (AbstractFilterOperation operation : operations) {
                        if(operation instanceof UniVarFilterOperation){
                            //todo 实现单元计算
                            operation.instantiateParameter(Collections.singletonList(schema.getColumn(((UniVarFilterOperation) operation).getColumnName())));
                        }
                    }
                }
            }
        }
        // todo 多元计算
    }
}
