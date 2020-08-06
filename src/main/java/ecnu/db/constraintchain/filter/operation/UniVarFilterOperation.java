package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.AbstractColumn;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    private CompareOperator leftOperator;
    private List<Parameter> leftParameters = new ArrayList<>();
    private CompareOperator rightOperator;
    private List<Parameter> rightParameters = new ArrayList<>();

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
    public static void merge(List<BoolExprNode> toMergeNodes, Multimap<String, UniVarFilterOperation> col2uniFilters) {
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
                newFilter.setLessOperator(typ2Filter.containsKey(CompareOperator.LE) ? CompareOperator.LE : CompareOperator.LT);
                newFilter.setGreaterOperator(typ2Filter.containsKey(CompareOperator.GE) ? CompareOperator.GE : CompareOperator.GT);
                toMergeNodes.add(newFilter);
            } else if (types.contains(CompareOperator.TYPE.LESS) && !types.contains(CompareOperator.TYPE.GREATER)) {
                RangeFilterOperation newFilter = new RangeFilterOperation(colName);
                newFilter.addLessParameters(Stream.concat(typ2Filter.get(CompareOperator.LE).stream(), typ2Filter.get(CompareOperator.LT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                newFilter.setLessOperator(typ2Filter.containsKey(CompareOperator.LE) ? CompareOperator.LE : CompareOperator.LT);
                toMergeNodes.add(newFilter);
            } else if (types.contains(CompareOperator.TYPE.GREATER) && !types.contains(CompareOperator.TYPE.LESS)) {
                RangeFilterOperation newFilter = new RangeFilterOperation(colName);
                newFilter.addGreaterParameters(Stream.concat(typ2Filter.get(CompareOperator.GE).stream(), typ2Filter.get(CompareOperator.GT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                newFilter.setGreaterOperator(typ2Filter.containsKey(CompareOperator.GE) ? CompareOperator.GE : CompareOperator.GT);
                toMergeNodes.add(newFilter);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * todo 参数实例化
     */
    @Override
    public void instantiateParameter(List<AbstractColumn> columns) {
        throw new NotImplementedException();
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
