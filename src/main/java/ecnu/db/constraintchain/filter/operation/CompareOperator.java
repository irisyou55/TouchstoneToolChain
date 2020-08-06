package ecnu.db.constraintchain.filter.operation;

/**
 * @author wangqingshuai
 * 比较算子Tag
 * 顺序不能改变，用于算子的合并，
 */

public enum CompareOperator {
    /**
     * 大于或等于
     */
    GE(TYPE.GREATER),
    /**
     * 比较运算符，大于
     */
    GT(TYPE.GREATER),
    /**
     * 小于或等于
     */
    LE(TYPE.LESS),
    /**
     * 比较运算符，小于
     */
    LT(TYPE.LESS),
    /**
     * 比较运算符，等于
     */
    EQ(TYPE.OTHER),
    /**
     * 比较运算符，不等于
     */
    NE(TYPE.OTHER),
    /**
     * 比较运算符，相似
     */
    LIKE(TYPE.OTHER),
    /**
     * 比较运算符，包含
     */
    IN(TYPE.OTHER),
    /**
     * ISNULL运算符
     */
    ISNULL(TYPE.OTHER),
    /**
     * RANGE运算符，表示多个lt,gt,le,ge的整合，不直接在parser中使用
     */
    RANGE(TYPE.OTHER);

    private final TYPE type;
    public enum TYPE {
        LESS, GREATER, OTHER
    }
    private CompareOperator(TYPE type) {
        this.type = type;
    }
    public TYPE getType() {
        return type;
    }
}
