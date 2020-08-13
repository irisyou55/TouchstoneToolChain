package ecnu.db.constraintchain.chain;

import ecnu.db.constraintchain.filter.Parameter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangqingshuai
 */
public class ConstraintChain {

    private String tableName;
    private List<Parameter> parameters = new ArrayList<>();
    private final List<ConstraintChainNode> nodes = new ArrayList<>();

    public ConstraintChain() {}

    public ConstraintChain(String tableName) {
        this.tableName = tableName;
    }

    public void addNode(ConstraintChainNode node) {
        nodes.add(node);
    }

    public List<ConstraintChainNode> getNodes() {
        return nodes;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void addParameters(List<Parameter> parameters) {
        this.parameters.addAll(parameters);
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return "{tableName:" + tableName + ",nodes:" + nodes + "}";
    }
}
