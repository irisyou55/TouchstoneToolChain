package ecnu.db.constraintchain.filter.operation;

/**
 * @author wangqingshuai
 * 比较算子Tag
 * 顺序不能改变，用于算子的合并，必须保证 GE < GT < EQ < LE < LT
 */

public enum CompareOperator {
    /**
     * 大于或等于
     */
    GE,
    /**
     * 比较运算符，大于
     */
    GT,
    /**
     * 比较运算符，等于
     */
    EQ,
    /**
     * 小于或等于
     */
    LE,
    /**
     * 比较运算符，小于
     */
    LT,
    /**
     * 比较运算符，不等于
     */
    NE,
    /**
     * 比较运算符，相似
     */
    LIKE,
    /**
     * 比较运算符，包含
     */
    IN,
    /**
     * ISNULL运算符
     */
    ISNULL
}
