package ecnu.db.schema.column;

import java.math.BigDecimal;
import java.time.Duration;
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

    @Override
    protected String generateRandomData(BigDecimal minProbability, BigDecimal maxProbability) {
        Duration duration = Duration.between(begin, end);
        BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
        BigDecimal probability = BigDecimal.valueOf(Math.random() * (maxProbability.doubleValue() - minProbability.doubleValue()) + minProbability.doubleValue());
        duration = Duration.ofSeconds(seconds.multiply(probability).longValue());
        return FMT.format(begin.plus(duration));
    }

    public LocalDate generateData(BigDecimal probability) {
        Duration duration = Duration.between(begin, end);
        BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
        duration = Duration.ofSeconds(seconds.multiply(probability).longValue());
        return begin.plus(duration);
    }
}
