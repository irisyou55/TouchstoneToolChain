package ecnu.db.schema.column;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractColumn {
    private final ColumnType columnType;
    protected float nullPercentage;
    protected String columnName;
    // 非等值约束
    protected NonEqBucket bucket;
    // 已经处理过的约束
    protected Set<String> metConditions = new HashSet<>();
    // 所有的非等值约束划分而成的等值约束的区间
    @JsonIgnore
    protected List<EqBucket> eqBuckets = new ArrayList<>();
    // 生成的等于约束参数数据
    protected Set<String> eqCandidates = new HashSet<>();

    public AbstractColumn(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
        bucket = new NonEqBucket();
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

    public List<EqBucket> getEqBuckets() {
        return eqBuckets;
    }

    /**
     * 插入非等值约束概率，按照lt来计算
     * @param probability 非等值约束概率
     * @param operator 操作符
     * @param value 参数对应的数据
     */
    public void insertNonEqProbability(BigDecimal probability, CompareOperator operator, String value) {
        if (metConditions.contains(operator + value)) {
            return;
        }
        metConditions.add(operator + value);
        if (operator.getType() != LESS && operator.getType() != GREATER) {
            throw new UnsupportedOperationException();
        }
        NonEqBucket bucket = this.bucket.search(probability);
        bucket.value = value;
        bucket.probability = probability;
        bucket.leftBucket = new NonEqBucket();
        bucket.rightBucket = new NonEqBucket();
        if (bucket.child != null) {
            BigDecimal toFillCapacity = probability.subtract(bucket.child.leftBorder),
                    occupiedCapacity = bucket.child.rightBorder.subtract(bucket.child.leftBorder).subtract(bucket.child.capacity);
            if (bucket.child.capacity.compareTo(toFillCapacity) < 0 && occupiedCapacity.compareTo(toFillCapacity) > 0) {
                throw new UnsupportedOperationException();
            }
            if (bucket.child.capacity.compareTo(toFillCapacity) >= 0) {
                bucket.leftBucket.child = new EqBucket(bucket, toFillCapacity, bucket.child.leftBorder, probability);
                bucket.rightBucket.child = bucket.child;
                bucket.rightBucket.child.capacity = bucket.rightBucket.child.capacity.subtract(toFillCapacity);
                bucket.rightBucket.child.leftBorder = probability;
                bucket.child = null;
                eqBuckets.add(bucket.leftBucket.child);
            }
            else if (occupiedCapacity.compareTo(toFillCapacity) <= 0) {
                bucket.rightBucket.child = new EqBucket(bucket, bucket.child.capacity.subtract(toFillCapacity.subtract(occupiedCapacity)), bucket.child.leftBorder, probability);
                bucket.leftBucket.child = bucket.child;
                bucket.leftBucket.child.capacity = bucket.child.capacity.subtract(toFillCapacity.subtract(occupiedCapacity));
                bucket.leftBucket.child.rightBorder = probability;
                bucket.child = null;
                eqBuckets.add(bucket.rightBucket.child);
            }
        }
    }

    /**
     * 从non-eq bucket划分的区间里生成eq-bucket
     */
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
                eqBucket.capacity = max.subtract(min);
                eqBucket.leftBorder = min;
                eqBucket.rightBorder = max;
                bucket.child = eqBucket;
                eqBuckets.add(eqBucket);
            }
            if (bucket.leftBucket != null) {
                buckets.push(Pair.of(Pair.of(min, bucket.probability), bucket.leftBucket));
            }
            if (bucket.rightBucket != null) {
                buckets.push(Pair.of(Pair.of(bucket.probability, max), bucket.rightBucket));
            }
        }
        eqBuckets.sort(Comparator.comparing(b -> b.capacity));
    }

    public void initEqParameter() {
        eqBuckets.sort(Comparator.comparing(b -> b.leftBorder));
        for (EqBucket eqBucket : eqBuckets) {
            eqBucket.eqConditions.forEach((b, param) -> {
                String data = generateEqData(eqBucket.leftBorder, eqBucket.rightBorder);
                param.setData(data);
            });
        }
    }

    public void insertEqualProbability(BigDecimal probability, Parameter parameter) {
        EqBucket eqBucket = fitProbability(probability);
        if (eqBucket.capacity.compareTo(probability) >= 0) {
            eqBucket.eqConditions.put(probability, parameter);
            eqBucket.capacity = eqBucket.capacity.subtract(probability);
        } else {
            eqBucket.eqConditions.put(eqBucket.capacity, parameter);
            eqBucket.capacity = BigDecimal.ZERO;
        }
        eqBuckets.sort(Comparator.comparing(o -> o.capacity));
    }

    private EqBucket fitProbability(BigDecimal probability) {
        int low = 0, high = eqBuckets.size() - 1;
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

    protected abstract String generateEqData(BigDecimal minProbability, BigDecimal maxProbability);

    public void addCondition(String condition) {
        metConditions.add(condition);
    }

    public boolean hasNotMetCondition(String condition) {
        return !metConditions.contains(condition);
    }

    /**
     * 插入between的概率
     * @param probability between的概率
     * @param lessParameters 代表between的小于条件的参数
     * @param greaterParameters 代表between的大于条件的参数
     */
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
        insertNonEqProbability(leftProbability, LT, leftData);
        insertNonEqProbability(rightProbability, LT, rightData);
    }

    /**
     * 根据概率生成非等值filter的参数数据
     * @param probability 分割概率
     * @return 生成的数据
     */
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
            DateTimeFormatterBuilder builder;
            builder = new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss");
            if (column.getPrecision() > 0) {
                builder.appendFraction(ChronoField.MICRO_OF_SECOND, 0, column.getPrecision(), true);
            }
            DateTimeFormatter formatter = builder.toFormatter();
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
