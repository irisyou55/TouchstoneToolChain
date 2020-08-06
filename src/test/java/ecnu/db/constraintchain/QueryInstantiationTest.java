package ecnu.db.constraintchain;

import com.fasterxml.jackson.databind.ObjectMapper;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.constraintchain.chain.ConstraintChainNode;
import ecnu.db.constraintchain.chain.ConstraintChainReader;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import org.junit.jupiter.api.Test;

import java.util.List;

class QueryInstantiationTest {
    @Test
    public void getOperationsTest() throws Exception {
        List<ConstraintChain> chains = ConstraintChainReader.readConstraintChain("src/test/resources/data/query-instantiation/constraintChain.json");
        for (ConstraintChain chain: chains) {
            for (ConstraintChainNode node: chain.getNodes()) {
                if (node instanceof ConstraintChainFilterNode) {
                    List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).pushDownProbability();
                    System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(operations));
                }
            }
        }
    }
}