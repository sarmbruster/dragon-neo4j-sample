package org.neo4j.dragon;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collections;

/**
 * Created by stefan on 28.02.17.
 */
public class ManualIndexTest {

    private GraphDatabaseService db;

    @Before
    public void setup() {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testManualIndexes() {
        Index<Node> myIndex = null;
        try (Transaction tx = db.beginTx()) {
            myIndex = db.index().forNodes("myIndex", Collections.singletonMap("type", "fulltext"));

            Node node = db.createNode();
            node.setProperty("name", "Stefan");
            myIndex.add(node, "name", "Stefan Armbruster");
            myIndex.add(node, "car", "Suzuki");
            tx.success();
        }

        try (Transaction tx = db.beginTx()) {
            final IndexHits<Node> hits = myIndex.query("name:Stefan");

            for (Node n : hits) {
                System.out.println(n.getProperty("name"));

            }

        }
        final Result result = db.execute("START n=node:myIndex('name:Armbruster') RETURN n");
        System.out.println(result.resultAsString());
    }

}
