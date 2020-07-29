package ecnu.db.utils.exception;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public class CannotFindColumnException extends TouchstoneToolChainException {
    public CannotFindColumnException(String tableName, String columnName) {
        super(String.format("表'%s'找不到'%s'对应的Column", tableName, columnName));
    }
}
