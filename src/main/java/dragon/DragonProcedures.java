package dragon;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by stefan on 02.03.17.
 */
public class DragonProcedures {

    private static final Node POISON = new NodeProxy(null, -1);

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Context
    public Log log;

    @Procedure(mode = Mode.WRITE)
    public void migrateAddProperty(@Name("cypher statement to operate on") String cypher,
                                   @Name("name of property to be added") String propertyName,
                                   @Name("batchsize") long batchSize) {

        Result result = graphDatabaseAPI.execute(cypher);
        if (result.columns().size() != 1) {
            throw new RuntimeException("expecting 1 result column in " + cypher);
        }

        BlockingQueue<Node> queue = new ArrayBlockingQueue<Node>((int)batchSize*2);
        
        spawnWorkThreadOnQueue(graphDatabaseAPI, queue, propertyName, batchSize);

        result.stream().forEach(stringObjectMap -> {
            final Object o = stringObjectMap.values().stream().findFirst().get();
            if (! (o instanceof Node)) {
                throw new RuntimeException("expecting a node as result in " + cypher);
            }
            Node n = (Node) o;
            try {
                queue.put(n);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            queue.put(POISON);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private void spawnWorkThreadOnQueue(GraphDatabaseAPI api, BlockingQueue<Node> queue, String propertyName, long batchSize) {
        new Thread(() -> {
            Node n;
            long opsCount = 0;
            Transaction tx = null;
            try {
                while (!(n = queue.take()).equals(POISON)) {
                    if (opsCount % batchSize == 0) {
                        if (tx!=null) {
                            tx.success();
                            tx.close();
                        }
                        tx = api.beginTx();
                        log.warn("rolling over transaction at opscount " + opsCount);
                    }

                    n.setProperty(propertyName, n.getProperty("first_name", null) + " " + n.getProperty("last_name", null));
                    opsCount ++;

                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                if (tx!=null) {
                    tx.success();
                    tx.close();
                }
            }

        }).start();
    }



    @UserFunction
    public boolean extProp(@Name("node") Node node,
                           @Name("externalPropertyKey") String externalPropertyKey) {

        // simplistic code ahead,
        // normally you would do: externalSystem.getValueForNode(node.getId(), externalPropertyKey)
        Map<String, Boolean> enabledData = new HashMap<>();
        enabledData.put("a", true);
        enabledData.put("b", true);
        enabledData.put("c", true);
        enabledData.put("d", true);
        enabledData.put("e", false);

        String key = (String) node.getProperty("id");
        return enabledData.get(key);
    }

}
