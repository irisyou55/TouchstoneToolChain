package ecnu.db.constraintchain.filter;

/**
 * @author alan
 * 代表需要实例化的参数
 */
public class Parameter {
    /**
     * parameter的id，用于后续实例化
     */
    private Integer id;
    /**
     * parameter的data
     */
    private String data;
    /**
     * 在SQL中是否需要加"'"符号，如字符串类型
     */
    private boolean needQuote;
    /**
     * 是否是Date类型的参数
     */
    private boolean isDate;

    public Parameter(Integer id, String data, boolean needQuote, boolean isDate) {
        this.id = id;
        this.data = data;
        this.needQuote = needQuote;
        this.isDate = isDate;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public boolean isNeedQuote() {
        return needQuote;
    }

    public void setNeedQuote(boolean needQuote) {
        this.needQuote = needQuote;
    }

    public boolean isDate() {
        return isDate;
    }

    public void setDate(boolean date) {
        isDate = date;
    }

    @Override
    public String toString() {
        return String.format("{id:%d, data:%s}", id, needQuote ? String.format("'%s'", data): data);
    }
}
