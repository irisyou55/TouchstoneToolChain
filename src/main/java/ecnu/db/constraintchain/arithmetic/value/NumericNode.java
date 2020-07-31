package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class NumericNode extends ArithmeticNode {
    public NumericNode() {
        super(ArithmeticNodeType.CONSTANT);
    }
    private Float constant;

    public void setConstant(float constant) {
        this.constant = constant;
    }

    public void setConstant(int constant) {
        this.constant = (float) constant;
    }

    public Float getConstant() {
        return constant;
    }

    @Override
    public float[] getVector() {
        int size = ArithmeticNode.size;
        float[] value = new float[size];
        Arrays.fill(value, constant);
        return value;
    }

    @Override
    public String toString() {
        return constant.toString();
    }
}
