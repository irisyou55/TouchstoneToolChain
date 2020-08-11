package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.column.AbstractColumn;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author wangqingshuai
 */
public class ColumnNode extends ArithmeticNode {
    private String canonicalTableName;
    private AbstractColumn column;
    private float min;
    private float max;

    public ColumnNode() {
        super(ArithmeticNodeType.COLUMN);
    }

    public ColumnNode(AbstractColumn column) {
        super(ArithmeticNodeType.COLUMN);
        this.column = column;
    }

    public void setMinMax(float min, float max) throws TouchstoneToolChainException {
        if (min > max) {
            throw new TouchstoneToolChainException("非法的随机生成定义");
        }
        this.min = min;
        this.max = max;
    }

    public AbstractColumn getColumn() {
        return column;
    }

    public void setColumn(AbstractColumn column) {
        this.column = column;
    }

    public String getCanonicalTableName() {
        return canonicalTableName;
    }

    public void setCanonicalTableName(String canonicalTableName) {
        this.canonicalTableName = canonicalTableName;
    }

    @Override
    public float[] getVector() {
        throw new NotImplementedException();
        //        List<EqBucket> eqBucketList = column.getEqBuckets();
//        int size = ArithmeticNode.size;
//        float[] value = new float[size];
//        float bound = max - min;
//        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
//        for (int i = 0; i < size; i++) {
//            value[i] = threadLocalRandom.nextFloat() * bound + min;
//        }
//        return value;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", canonicalTableName, column.getColumnName());
    }
}
