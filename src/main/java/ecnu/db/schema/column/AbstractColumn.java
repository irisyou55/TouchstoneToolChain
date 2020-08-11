package ecnu.db.schema.column;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.schema.column.bucket.EqBucket;
import ecnu.db.schema.column.bucket.NonEqBucket;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.LT;
import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.GREATER;
import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.LESS;
import static ecnu.db.schema.column.ColumnType.*;


/**
 * @author qingshuai.wang
 */
public abstract class AbstractColumn {
    private final ColumnType columnType;
    protected float nullPercentage;
    protected String columnName;
    protected NonEqBucket bucket;
    protected Set<String> metConditions = new HashSet<>();
    protected List<EqBucket> eqBuckets = new ArrayList<>();

    public AbstractColumn(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
        bucket = new NonEqBucket();
        bucket.probability = BigDecimal.ONE;
    }

    /**
     * 获取该列非重复值的个数
     *
     * @return 非重复值的个数
     */
    public abstract int getNdv();

    @JsonGetter
    @SuppressWarnings("unused")
    public float getNullPercentage() {
        return nullPercentage;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    @JsonSetter
    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }

    public void adjustNonEqProbabilityBucket(BigDecimal probability, CompareOperator operator, String value) {
        if (operator.getType() != LESS && operator.getType() != GREATER) {
            throw new UnsupportedOperationException();
        }
        NonEqBucket bucket = this.bucket.search(probability);
        bucket.value = value;
        bucket.probability = probability;
        bucket.leftBucket = new NonEqBucket();
        bucket.rightBucket = new NonEqBucket();
    }

    public void initEqProbabilityBucket() {
        Deque<Pair<Pair<BigDecimal, BigDecimal>, NonEqBucket>> buckets = new ArrayDeque<>();
        buckets.push(Pair.of(Pair.of(BigDecimal.ZERO, BigDecimal.ONE), bucket));
        while (!buckets.isEmpty()) {
            Pair<Pair<BigDecimal, BigDecimal>, NonEqBucket> pair = buckets.pop();
            NonEqBucket bucket = pair.getRight();
            BigDecimal min = pair.getLeft().getLeft(), max = pair.getLeft().getRight();
            if (bucket.leftBucket == null && bucket.rightBucket == null) {
                EqBucket eqBucket = new EqBucket();
                eqBucket.parent = bucket;
                eqBucket.capacity = bucket.probability;
                eqBucket.leftBorder = min;
                eqBucket.rightBorder = max;
                eqBuckets.add(eqBucket);
            }
            if (bucket.leftBucket != null) {
                buckets.push(Pair.of(Pair.of(min, bucket.probability), bucket.leftBucket));
            }
            if (bucket.rightBucket != null) {
                buckets.push(Pair.of(Pair.of(bucket.probability, max), bucket.rightBucket));
            }
        }
        eqBuckets.sort(Comparator.comparing(o -> o.capacity));
    }

    public void insertEqualProbability(BigDecimal probability, Parameter parameter) {
        EqBucket eqBucket = fitProbability(probability);
        String data = generateRandomData(eqBucket.leftBorder, eqBucket.rightBorder);
        parameter.setData(data);
        if (eqBucket.capacity.compareTo(probability) >= 0) {
            eqBucket.capacity = eqBucket.capacity.subtract(probability);
        } else {
            eqBucket.capacity = BigDecimal.ZERO;
        }
        eqBuckets.sort(Comparator.comparing(o -> o.capacity));
    }

    private EqBucket fitProbability(BigDecimal probability) {
        int low = 0, high = eqBuckets.size();
        while (low < high) {
            EqBucket bucket = eqBuckets.get((low + high) / 2);
            if (bucket.capacity.compareTo(probability) > 0) {
                high = (low + high) / 2;
            }
            else if (bucket.capacity.compareTo(probability) < 0) {
                low = (low + 1 + high) / 2;
            }
            else {
                return eqBuckets.get((low + high) / 2);
            }
        }
        return eqBuckets.get(low);
    }

    protected abstract String generateRandomData(BigDecimal minProbability, BigDecimal maxProbability);

    public void addCondition(String condition) {
        metConditions.add(condition);
    }

    public boolean hasNotMetCondition(String condition) {
        return !metConditions.contains(condition);
    }

    public void insertBetweenProbability(BigDecimal probability, List<Parameter> lessParameters, List<Parameter> greaterParameters) {
        BigDecimal minDeviation = BigDecimal.ONE, deviation;
        eqBuckets.sort(Comparator.comparing(b -> b.leftBorder));
        int i = 0, minIndex = 0;
        for (i = 0; i < eqBuckets.size(); i++) {
            EqBucket eqBucket = eqBuckets.get(i);
            BigDecimal rightBorder = eqBucket.leftBorder.add(probability);
            EqBucket rightBucket = fitProbability(rightBorder);
            if (rightBucket.leftBorder.add(rightBucket.capacity).compareTo(probability) >= 0) {
                minIndex = i;
                break;
            } else {
                deviation = probability.subtract(rightBucket.leftBorder.add(rightBucket.capacity));
                if (deviation.compareTo(minDeviation) < 0) {
                    minDeviation = deviation;
                    minIndex = i;
                }
            }
        }
        EqBucket eqBucket = eqBuckets.get(minIndex);
        BigDecimal rightBorder = eqBucket.leftBorder.add(probability);
        EqBucket rightBucket = fitProbability(rightBorder);
        double left = Double.min(eqBucket.capacity.doubleValue(), rightBucket.capacity.doubleValue());
        BigDecimal leftProbability = BigDecimal.valueOf((1 - Math.random()) * left), rightProbability = leftProbability.add(probability);
        String leftData = genData(leftProbability), rightData = genData(rightProbability);
        lessParameters.forEach((p) -> p.setData(leftData));
        greaterParameters.forEach((p) -> p.setData(rightData));
        adjustNonEqProbabilityBucket(leftProbability, LT, leftData);
        adjustNonEqProbabilityBucket(rightProbability, LT, rightData);
    }

    public String genData(BigDecimal probability) {
        String ret;
        if (getColumnType() == INTEGER) {
            IntColumn column = (IntColumn) this;
            int value = column.generateData(probability);
            ret = Integer.toString(value);
        }
        else if (getColumnType() == DECIMAL) {
            DecimalColumn column = (DecimalColumn) this;
            BigDecimal value = column.generateData(probability);
            ret = value.toString();
        }
        else if (getColumnType() == DATETIME) {
            DateTimeColumn column = (DateTimeColumn) this;
            LocalDateTime newDateTime = column.generateData(probability);
            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.MICRO_OF_SECOND, 0, column.getPrecision(), true).toFormatter();
            return formatter.format(newDateTime);
        }
        else if (getColumnType() == DATE) {
            DateColumn column = (DateColumn) this;
            LocalDate newDate = column.generateData(probability);
            DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter();
            return formatter.format(newDate);
        }
        else {
            throw new UnsupportedOperationException();
        }

        return ret;
    }
}
