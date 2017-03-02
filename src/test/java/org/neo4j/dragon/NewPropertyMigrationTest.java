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
public class NewPropertyMigrationTest {

    private GraphDatabaseService graphDatabaseService;

    @Before
    public void setup() throws KernelException {
        graphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final Procedures procedures = ((GraphDatabaseAPI) graphDatabaseService).getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(DragonProcedures.class);
    }

    @After
    public void teardown() {
        graphDatabaseService.shutdown();
    }

    @Test
    public void testAddName() {
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

}
