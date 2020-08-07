package ecnu.db.schema.column;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * @author alan
 */
public class DateColumn extends AbstractColumn {
    public static final DateTimeFormatter FMT = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter();
    private LocalDate begin;
    private LocalDate end;

    public DateColumn() {
        super(null, ColumnType.DATETIME);
    }

    public DateColumn(String columnName) {
        super(columnName, ColumnType.DATETIME);
    }

    public LocalDate getBegin() {
        return begin;
    }

    public void setBegin(LocalDate begin) {
        this.begin = begin;
    }

    public LocalDate getEnd() {
        return end;
    }

    public void setEnd(LocalDate end) {
        this.end = end;
    }

    @Override
    public int getNdv() {
        return -1;
    }
}
