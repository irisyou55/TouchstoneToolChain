package ecnu.db.constraintchain;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.chain.*;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.constraintchain.filter.operation.MultiVarFilterOperation;
import ecnu.db.constraintchain.filter.operation.RangeFilterOperation;
import ecnu.db.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.utils.CommonUtils;

import java.util.List;
import java.util.Map;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.*;

/**
 * @author wangqingshuai
 */
public class QueryInstantiation {
    public static void compute(List<ConstraintChain> constraintChains, Map<String, Schema> schemas) throws TouchstoneToolChainException {
        //todo 1. 对于数值型的filter, 首先计算单元的filter, 然后计算多值的filter，
        //        对于bet操作，先记录阈值，然后选择合适的区间插入，等值约束也需选择合适的区间
        //        每个filter operation内部保存自己实例化后的结果
        //     2. 对于字符型的filter, 只有like和eq的运算，直接计算即可
        Multimap<Schema, AbstractFilterOperation> schema2filters = ArrayListMultimap.create();
        for (ConstraintChain constraintChain : constraintChains) {
            Schema schema = schemas.get(constraintChain.getTableName());
            for (ConstraintChainNode node : constraintChain.getNodes()) {
                if (node instanceof ConstraintChainFilterNode) {
                    List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).pushDownProbability();
                    schema2filters.putAll(schema, operations);
                }
//                else if (node instanceof ConstraintChainPkJoinNode) {
//                    throw new UnsupportedOperationException();
//                }
//                else if (node instanceof ConstraintChainFkJoinNode) {
//                    throw new UnsupportedOperationException();
//                }
//                else {
//                    throw new UnsupportedOperationException();
//                }
            }
        }
        // uni-var non-eq
        for (Map.Entry<Schema, AbstractFilterOperation> entry : schema2filters.entries()) {
            Schema schema = entry.getKey();
            if (entry.getValue() instanceof UniVarFilterOperation
                    && (entry.getValue().getOperator().getType() == LESS || entry.getValue().getOperator().getType() == GREATER)) {
                UniVarFilterOperation operation = (UniVarFilterOperation) entry.getValue();
                String columnName = CommonUtils.extractSimpleColumnName(operation.getColumnName());
                AbstractColumn column = schema.getColumn(columnName);
                operation.instantiateUniParamCompParameter(column);
            }
        }
        schemas.forEach((s1, schema) -> {
            schema.getColumns().forEach((s2, col) -> {col.initEqProbabilityBucket();});
        });
        // uni-var eq
        for (Map.Entry<Schema, AbstractFilterOperation> entry : schema2filters.entries()) {
            Schema schema = entry.getKey();
            if (entry.getValue() instanceof UniVarFilterOperation
                    && (entry.getValue().getOperator().getType() == EQUAL)) {
                UniVarFilterOperation operation = (UniVarFilterOperation) entry.getValue();
                String columnName = CommonUtils.extractSimpleColumnName(operation.getColumnName());
                AbstractColumn column = schema.getColumn(columnName);
                operation.instantiateEqualParameter(column);
            }
        }
        // generate column bucket
        schemas.values().forEach((s) -> s.getColumns().values().forEach(AbstractColumn::initEqProbabilityBucket));
        // uni-var bet
        for (Map.Entry<Schema, AbstractFilterOperation> entry : schema2filters.entries()) {
            Schema schema = entry.getKey();
            if (entry.getValue() instanceof RangeFilterOperation) {
                RangeFilterOperation operation = (RangeFilterOperation) entry.getValue();
                String columnName = CommonUtils.extractSimpleColumnName(operation.getColumnName());
                AbstractColumn column = schema.getColumn(columnName);
                operation.instantiateBetweenParameter(column);
            }
        }
        // uni-var eq params init
        schemas.values().forEach((s) -> s.getColumns().values().forEach(AbstractColumn::initEqParameter));
        // multi-var non-eq
        for (Map.Entry<Schema, AbstractFilterOperation> entry : schema2filters.entries()) {
            if (entry.getValue() instanceof MultiVarFilterOperation) {
                MultiVarFilterOperation operation = (MultiVarFilterOperation) entry.getValue();
                operation.instantiateMultiVarParameter();
            }
        }

    }

}
