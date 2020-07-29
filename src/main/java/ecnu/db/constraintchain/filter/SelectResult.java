package ecnu.db.constraintchain.filter;

import ecnu.db.constraintchain.filter.logical.AndNode;

import java.util.List;

/**
 * @author alan
 */
public class SelectResult {
    AndNode condition;
    List<Parameter> parameters;
    String tableName;

    public AndNode getCondition() {
        return condition;
    }

    public void setCondition(AndNode condition) {
        this.condition = condition;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
