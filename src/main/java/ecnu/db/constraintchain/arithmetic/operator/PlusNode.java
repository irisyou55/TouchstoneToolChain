package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;

/**
 * @author wangqingshuai
 */
public class PlusNode extends ArithmeticNode {
    public PlusNode() {
        super(ArithmeticNodeType.PLUS);
    }
    @Override
    public float[] getVector() {
        float[] leftValue = leftNode.getVector(), rightValue = rightNode.getVector();
        for (int i = 0; i < leftValue.length; i++) {
            leftValue[i] += rightValue[i];
        }
        return leftValue;
    }

    @Override
    public String toString() {
        return String.format("plus(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
