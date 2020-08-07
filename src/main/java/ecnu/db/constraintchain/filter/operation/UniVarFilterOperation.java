package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author wangqingshuai
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UniVarFilterOperation extends AbstractFilterOperation {
    private String columnName;
    private Boolean hasNot = false;

    public UniVarFilterOperation() {
        super(null);
    }

    public UniVarFilterOperation(String columnName, CompareOperator operator) {
        super(operator);
        this.columnName = columnName;
    }

    /**
     * merge operation
     */
    public static void merge(List<BoolExprNode> toMergeNodes, Multimap<String, UniVarFilterOperation> col2uniFilters, boolean isAnd) {
        for (String colName: col2uniFilters.keySet()) {
            Collection<UniVarFilterOperation> filters = col2uniFilters.get(colName);
            Multimap<CompareOperator, UniVarFilterOperation> typ2Filter = Multimaps.index(filters, AbstractFilterOperation::getOperator);
            Set<CompareOperator.TYPE> types = typ2Filter.asMap().keySet().stream().map(CompareOperator::getType).collect(Collectors.toSet());
            if (typ2Filter.size() == 1) {
                toMergeNodes.add((BoolExprNode) CollectionUtils.get(typ2Filter.values(), 0));
                continue;
            }
            if (types.contains(CompareOperator.TYPE.GREATER) && types.contains(CompareOperator.TYPE.LESS)) {
                RangeFilterOperation newFilter = new RangeFilterOperation(colName);
                newFilter.addLessParameters(Stream.concat(typ2Filter.get(CompareOperator.LE).stream(), typ2Filter.get(CompareOperator.LT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                newFilter.addGreaterParameters(Stream.concat(typ2Filter.get(CompareOperator.GE).stream(), typ2Filter.get(CompareOperator.GT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                setLessOperator(isAnd, typ2Filter, newFilter);
                setGreaterOperator(isAnd, typ2Filter, newFilter);
                toMergeNodes.add(newFilter);
            } else if (types.contains(CompareOperator.TYPE.LESS) && !types.contains(CompareOperator.TYPE.GREATER)) {
                RangeFilterOperation newFilter = new RangeFilterOperation(colName);
                newFilter.addLessParameters(Stream.concat(typ2Filter.get(CompareOperator.LE).stream(), typ2Filter.get(CompareOperator.LT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                setGreaterOperator(isAnd, typ2Filter, newFilter);
                toMergeNodes.add(newFilter);
            } else if (types.contains(CompareOperator.TYPE.GREATER) && !types.contains(CompareOperator.TYPE.LESS)) {
                RangeFilterOperation newFilter = new RangeFilterOperation(colName);
                newFilter.addGreaterParameters(Stream.concat(typ2Filter.get(CompareOperator.GE).stream(), typ2Filter.get(CompareOperator.GT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                setLessOperator(isAnd, typ2Filter, newFilter);
                toMergeNodes.add(newFilter);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static void setLessOperator(boolean isAnd, Multimap<CompareOperator, UniVarFilterOperation> typ2Filter, RangeFilterOperation newFilter) {
        if (isAnd) {
            newFilter.setGreaterOperator(typ2Filter.containsKey(CompareOperator.GT) ? CompareOperator.GT : CompareOperator.GE);
        } else {
            newFilter.setGreaterOperator(typ2Filter.containsKey(CompareOperator.GE) ? CompareOperator.GE : CompareOperator.GT);
        }
    }

    private static void setGreaterOperator(boolean isAnd, Multimap<CompareOperator, UniVarFilterOperation> typ2Filter, RangeFilterOperation newFilter) {
        if (isAnd) {
            newFilter.setLessOperator(typ2Filter.containsKey(CompareOperator.LT) ? CompareOperator.LT : CompareOperator.LE);
        } else {
            newFilter.setLessOperator(typ2Filter.containsKey(CompareOperator.LE) ? CompareOperator.LE : CompareOperator.LT);
        }
    }

    @Override
    public void instantiateParameter(Map<String, AbstractColumn> columns) {
        AbstractColumn absColumn = columns.get(columnName);
        if (operator.getType() == CompareOperator.TYPE.LESS || operator.getType() == CompareOperator.TYPE.GREATER) {
            probability = operator.getType() == CompareOperator.TYPE.LESS ? probability : BigDecimal.ONE.subtract(probability);
            if (absColumn.getColumnType() == ColumnType.INTEGER) {
                IntColumn column = (IntColumn) absColumn;
                int value =  BigDecimal.valueOf(column.getMax() - column.getMin()).multiply(probability).intValue();
                parameters.forEach((param) -> {
                    param.setData(Integer.toString(value));
                });
            }
            else if (absColumn.getColumnType() == ColumnType.DECIMAL) {
                DecimalColumn column = (DecimalColumn) absColumn;
                BigDecimal value =  BigDecimal.valueOf(column.getMax() - column.getMin()).multiply(probability);
                parameters.forEach((param) -> {
                    param.setData(value.toString());
                });
            }
            else if (absColumn.getColumnType() == ColumnType.DATETIME) {
                DateTimeColumn column = (DateTimeColumn) absColumn;
                Duration duration = Duration.between(column.getBegin(), column.getEnd());
                BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
                BigDecimal nano = BigDecimal.valueOf(duration.getNano());
                duration = Duration.ofSeconds(seconds.multiply(probability).longValue(), nano.multiply(probability).intValue());
                LocalDateTime newDateTime = column.getBegin().plus(duration);
                DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                        .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.MICRO_OF_SECOND, 0, column.getPrecision(), true).toFormatter();
                parameters.forEach((param) -> param.setData(formatter.format(newDateTime)));
            }
            else if (absColumn.getColumnType() == ColumnType.DATE) {
                DateColumn column = (DateColumn) absColumn;
                Duration duration = Duration.between(column.getBegin(), column.getEnd());
                BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
                duration = Duration.ofSeconds(seconds.multiply(probability).longValue());
                LocalDate newDate = column.getBegin().plus(duration);
                DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter();
                parameters.forEach((param) -> param.setData(formatter.format(newDate)));
            }
            else {
                throw new UnsupportedOperationException();
            }
        }
        else if (operator.getType() == CompareOperator.TYPE.OTHER) {
            throw new NotImplementedException();
        }
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    public Boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(Boolean hasNot) {
        this.hasNot = hasNot;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String toString() {
        List<String> content = parameters.stream().map(Parameter::toString).collect(Collectors.toList());
        content.add(0, String.format("%s", columnName));
        if (hasNot) {
            return String.format("not(%s(%s))", operator.toString().toLowerCase(), String.join(", ", content));
        }
        return String.format("%s(%s)", operator.toString().toLowerCase(), String.join(", ", content));
    }
}
