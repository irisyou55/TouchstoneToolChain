package ecnu.db.exception;

/**
 * @author alan
 */
public class UnsupportedDBTypeException extends TouchstoneToolChainException {

    public UnsupportedDBTypeException(String dbType) {
        super(String.format("暂时不支持的数据库连接类型%s", dbType));
    }
}
