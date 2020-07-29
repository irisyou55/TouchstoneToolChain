package ecnu.db.utils.exception;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public class UnsupportedDatabaseSourceException extends TouchstoneToolChainException {
    public UnsupportedDatabaseSourceException(String dbSource) {
        super(String.format("暂时不支持的数据源类型'%s'", dbSource));
    }
}
