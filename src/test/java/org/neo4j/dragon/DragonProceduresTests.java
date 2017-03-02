package org.neo4j.dragon;

import dragon.DragonProcedures;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by stefan on 02.03.17.
 */
public class DragonProceduresTests {

    private GraphDatabaseService graphDatabaseService;

    @Before
    public void setup() throws KernelException {
        graphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final Procedures procedures = ((GraphDatabaseAPI) graphDatabaseService).getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(DragonProcedures.class);
        procedures.registerFunction(DragonProcedures.class);
    }

    @After
    public void teardown() {
        graphDatabaseService.shutdown();
    }

    @Test
    public void testAddingNewPropertyMigration() {
        // setup
        graphDatabaseService.execute("UNWIND range(1,10) as x CREATE (:User{first_name:'John_' + x, last_name:'Doe'})");

        // when
        graphDatabaseService.execute("call dragon.migrateAddProperty('match (u:User) return u', 'name', 2 )");

        // then
        final Result result = graphDatabaseService.execute("match (u:User) return u.name as name");
        List<String> names = Iterators.asList(result.columnAs("name"));
        Assert.assertEquals(10, names.size());
        Pattern p = Pattern.compile("John_\\d+ Doe");

        Assert.assertTrue(names.stream().allMatch(s -> p.matcher(s).matches()));
    }

    @Test
    public void testPathConditionsWithExternalProperties() {
        // setup

        graphDatabaseService.execute("CREATE (a{id:'a'})-[:KNOWS]->(b{id:'b'})-[:KNOWS]->(c{id:'c'})-[:KNOWS]->(d{id:'d'}), (a)-[:KNOWS]->(e{id:'e'})-[:KNOWS]->(d)");

        // when
        Result result = graphDatabaseService.execute("MATCH path=shortestPath( (a {id:'a'})-[*]-(d {id:'d'}) ) " +
                "where all(x in nodes(path) WHERE dragon.extProp(x, 'enabled')) " +
                "return [x in nodes(path) | x.id] as ids");

        // then
        final List<String> ids = Iterators.single(result.columnAs("ids"));

        Assert.assertEquals("[a, b, c, d]", ids.toString());

        // pseudo code for a user defined function acting as explicit external index
        //"MATCH (a) WHERE id(a)=dragon.getNodeIdFromExternalIndex('abc') "
    }

}
