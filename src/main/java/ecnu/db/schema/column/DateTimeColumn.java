package ecnu.db.schema.column;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * @author qingshuai.wang
 */
public class DateTimeColumn extends AbstractColumn {
    public static final DateTimeFormatter FMT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true).toFormatter();
    private LocalDateTime begin;
    private LocalDateTime end;
    private int precision; // fraction precision for datetime

    public DateTimeColumn() {
        super(null, ColumnType.DATETIME);
    }

    public DateTimeColumn(String columnName) {
        super(columnName, ColumnType.DATETIME);
    }

    public LocalDateTime getBegin() {
        return begin;
    }

    public void setBegin(LocalDateTime begin) {
        this.begin = begin;
    }

    public LocalDateTime getEnd() {
        return end;
    }

    public void setEnd(LocalDateTime end) {
        this.end = end;
    }

    @Override
    public int getNdv() {
        return -1;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }
}
