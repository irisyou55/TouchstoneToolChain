package ecnu.db.utils.exception;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public class UnsupportedJoin extends TouchstoneToolChainException {
    public UnsupportedJoin(String operatorInfo) {
        super(String.format("暂时不支持的join类型 operator_info:'%s'", operatorInfo));
    }
}
