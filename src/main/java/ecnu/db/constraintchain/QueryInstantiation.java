package ecnu.db.constraintchain;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.constraintchain.chain.ConstraintChainNode;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.constraintchain.filter.operation.MultiVarFilterOperation;
import ecnu.db.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author wangqingshuai
 */
public class QueryInstantiation {
    public static void compute(List<ConstraintChain> constraintChains, Map<String, Schema> schemas) throws TouchstoneToolChainException {
        //todo 1. 对于数值型的filter, 首先计算单元的filter, 然后计算多值的filter，
        //        对于bet操作，先记录阈值，然后选择合适的区间插入，等值约束也需选择合适的区间
        //        每个filter operation内部保存自己实例化后的结果
        //     2. 对于字符型的filter, 只有like和eq的运算，直接计算即可
        ArithmeticNode.setSize(10_000);
        for (ConstraintChain constraintChain : constraintChains) {
            Schema schema = schemas.get(constraintChain.getTableName());
            for (ConstraintChainNode node : constraintChain.getNodes().stream().filter((node) -> node instanceof ConstraintChainFilterNode).collect(Collectors.toList())) {
                List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).pushDownProbability();
                for (AbstractFilterOperation operation : operations) {
                    if(operation instanceof UniVarFilterOperation &&
                            (operation.getOperator().getType() == CompareOperator.TYPE.LESS || operation.getOperator().getType() == CompareOperator.TYPE.GREATER) ) {
                        operation.instantiateParameter(schema.getColumns());
                    }
                    else if (operation instanceof MultiVarFilterOperation) {
                        operation.instantiateParameter(schema.getColumns());
                    }
                }
            }
        }
        // generate column bucket
        for (Schema schema : schemas.values()) {
            for (AbstractColumn column : schema.getColumns().values()) {
                column.initEqProbabilityBucket();
            }
        }
    }
}
