package ecnu.db.constraintchain.filter;

/**
 * @author alan
 * 代表需要实例化的参数
 */
public class Parameter {
    private Integer id;
    private String data;
    private boolean needQuote;
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
        return "Parameter{" +
                "id=" + id +
                ", data='" + data + '\'' +
                '}';
    }
}
