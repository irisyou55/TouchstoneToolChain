package ecnu.db.constraintchain.filter.logical;

import ch.obermuhlner.math.big.BigDecimalMath;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.constraintchain.filter.operation.IsNullFilterOperation;
import ecnu.db.constraintchain.filter.operation.MultiVarFilterOperation;
import ecnu.db.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.constraintchain.filter.BoolExprType.*;

/**
 * @author wangqingshuai
 */
public class AndNode implements BoolExprNode {
    private BoolExprType type = BoolExprType.AND;
    private LinkedList<BoolExprNode> children;

    public AndNode() {
        this.children = new LinkedList<>();
    }

    public void addChild(BoolExprNode logicalNode) {
        children.add(logicalNode);
    }

    /**
     * todo 当前And计算概率时，未考虑单值算子和多值算子的相互影响
     * <p>
     * todo 当前And计算概率时，假设可以合并的operation在同一组children中
     *
     * @param probability 当前节点的总概率值
     */
    @Override
    public List<AbstractFilterOperation> calculateProbability(BigDecimal probability) throws TouchstoneToolChainException {
        BigDecimal nodeProbability = new BigDecimal(probability.toString());
        HashMap<String, UniVarFilterOperation> uniVarFilterOperationHashMap = new HashMap<>();
        LinkedList<BoolExprNode> logicalNodes = new LinkedList<>();
        LinkedList<MultiVarFilterOperation> multiVarFilterOperations = new LinkedList<>();
        LinkedList<IsNullFilterOperation> isNullFilterOperations = new LinkedList<>();
        HashSet<String> operationColNames = new HashSet<>();
        // 1. 分离各种node
        // 2. 合并单列的operation
        // 3. 记录operation访问的各个列名
        for (BoolExprNode child : children) {
            if (child.getType() == AND || child.getType() == OR) {
                logicalNodes.add(child);
            } else if (child.getType() == UNI_FILTER_OPERATION) {
                UniVarFilterOperation uniVarFilterOperation = (UniVarFilterOperation) child;
                if (!uniVarFilterOperationHashMap.containsKey(uniVarFilterOperation.getColumnName())) {
                    uniVarFilterOperationHashMap.put(uniVarFilterOperation.getColumnName(), uniVarFilterOperation);
                    operationColNames.add(uniVarFilterOperation.getColumnName());
                } else {
                    uniVarFilterOperationHashMap.get(uniVarFilterOperation.getColumnName()).merge(uniVarFilterOperation);
                }
            } else if (child.getType() == ISNULL_FILTER_OPERATION) {
                isNullFilterOperations.add((IsNullFilterOperation) child);
            } else {
                multiVarFilterOperations.add((MultiVarFilterOperation) child);
                operationColNames.addAll(((MultiVarFilterOperation) child).getColNames());
            }
        }
        // 如果存在表达式的话，not isnull的约束不应该再被考虑，而isnull是相斥约束，会导致概率为0，应该抛出异常
        // 对于合法的null约束，应该重新计算and算子的总概率
        Iterator<IsNullFilterOperation> isNullFilterOperationIterator = isNullFilterOperations.iterator();
        while (isNullFilterOperationIterator.hasNext()) {
            IsNullFilterOperation isNullFilterOperation = isNullFilterOperationIterator.next();
            if (isNullFilterOperation.getHasNot()) {
                if (operationColNames.contains(isNullFilterOperation.getColumnName())) {
                    isNullFilterOperationIterator.remove();
                } else {
                    BigDecimal filterOperationProbability = BigDecimal.ONE.subtract(BigDecimal.valueOf(isNullFilterOperation.getNullProbability()));
                    nodeProbability = nodeProbability.divide(filterOperationProbability, CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
                }
            } else {
                if (operationColNames.contains(isNullFilterOperation.getColumnName())) {
                    throw new TouchstoneToolChainException("isnull和operation互斥");
                } else {
                    BigDecimal filterOperationProbability = BigDecimal.valueOf(isNullFilterOperation.getNullProbability());
                    nodeProbability = nodeProbability.divide(filterOperationProbability, CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
                }
            }
        }

        // 归纳未被合并的node
        List<BoolExprNode> mergedExprNodes = new LinkedList<>();
        mergedExprNodes.addAll(uniVarFilterOperationHashMap.values());
        mergedExprNodes.addAll(multiVarFilterOperations);
        mergedExprNodes.addAll(logicalNodes);

        // 对总概率开mergedExprNodes.size次方
        BigDecimal rootForComputing = BigDecimal.ONE.divide(BigDecimal.valueOf(mergedExprNodes.size()), CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
        nodeProbability = BigDecimalMath.pow(nodeProbability, rootForComputing, CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);

        // 设置概率并递归计算
        List<AbstractFilterOperation> operations = new LinkedList<>();
        for (BoolExprNode logicalNode : mergedExprNodes) {
            operations.addAll(logicalNode.calculateProbability(nodeProbability));
        }
        return operations;
    }

    @Override
    public BoolExprType getType() {
        return type;
    }

    public void setType(BoolExprType type) {
        this.type = type;
    }

    public LinkedList<BoolExprNode> getChildren() {
        return children;
    }

    public void setChildren(LinkedList<BoolExprNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return String.format("and(%s)", children.stream().map(BoolExprNode::toString).collect(Collectors.joining(", ")));
    }
}
