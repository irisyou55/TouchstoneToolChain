package ecnu.db.schema.column;


import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * @author qingshuai.wang
 */
public class DateColumn extends AbstractColumn {
    private static final SimpleDateFormat TOUCHSTONE_FMT = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    private static final String[] DATA_TIME_PATTERN = new String[]{"yyyy-MM", "yyyyMM", "yyyy/MM", "yyyyMMdd", "yyyy-MM-dd", "yyyy/MM/dd",
            "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss"};
    private static final Pattern DATE_FORMAT_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");
    private String begin;
    private String end;

    public DateColumn(String columnName) {
        super(columnName, ColumnType.DATETIME);
    }

    public String getBegin() {
        return begin;
    }

    public void setBegin(String begin) {
        this.begin = begin;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    @Override
    public int getNdv() {
        return -1;
    }
}
