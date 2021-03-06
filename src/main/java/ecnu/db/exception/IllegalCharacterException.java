package ecnu.db.exception;

/**
 * @author alan
 */
public class IllegalCharacterException extends TouchstoneToolChainException {
    public IllegalCharacterException(String character, int line, long begin) {
        super(String.format("非法字符 %s at line:%d pos:%s", character, line, begin));
    }
}
