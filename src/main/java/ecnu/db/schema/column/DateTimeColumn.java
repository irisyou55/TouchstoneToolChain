package ecnu.db.schema.column;


import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * @author qingshuai.wang
 */
public class DateTimeColumn extends AbstractColumn {
    public static final DateTimeFormatter FMT = new DateTimeFormatterBuilder()
            .appendOptional(new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd")
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                    .toFormatter())
            .appendOptional(new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .toFormatter())
            .toFormatter();
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

    @Override
    protected String generateRandomData(BigDecimal minProbability, BigDecimal maxProbability) {
        Duration duration = Duration.between(getBegin(), getEnd());
        BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
        BigDecimal nano = BigDecimal.valueOf(duration.getNano());
        BigDecimal probability = BigDecimal.valueOf(Math.random() * (maxProbability.doubleValue() - minProbability.doubleValue()) + minProbability.doubleValue());
        duration = Duration.ofSeconds(seconds.multiply(probability).longValue(), nano.multiply(probability).intValue());
        return FMT.format(begin.plus(duration));
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public LocalDateTime generateData(BigDecimal probability) {
        Duration duration = Duration.between(getBegin(), getEnd());
        BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
        BigDecimal nano = BigDecimal.valueOf(duration.getNano());
        duration = Duration.ofSeconds(seconds.multiply(probability).longValue(), nano.multiply(probability).intValue());
        return begin.plus(duration);
    }
}
