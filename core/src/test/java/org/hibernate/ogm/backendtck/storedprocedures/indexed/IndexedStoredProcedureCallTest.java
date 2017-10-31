/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.storedprocedures.indexed;

import static org.fest.assertions.Assertions.assertThat;
import static org.hibernate.ogm.utils.GridDialectType.INFINISPAN;
import static org.hibernate.ogm.utils.GridDialectType.INFINISPAN_REMOTE;
import static org.hibernate.ogm.utils.GridDialectType.NEO4J_EMBEDDED;
import static org.hibernate.ogm.utils.GridDialectType.NEO4J_REMOTE;

import java.util.List;
import java.util.Properties;
import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.StoredProcedureQuery;

import org.hibernate.ogm.backendtck.storedprocedures.Car;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.jpa.impl.OgmStoredProcedureQuery;
import org.hibernate.ogm.utils.GridDialectType;
import org.hibernate.ogm.utils.SkipByGridDialect;
import org.hibernate.ogm.utils.TestForIssue;
import org.hibernate.ogm.utils.TestHelper;
import org.hibernate.ogm.utils.jpa.GetterPersistenceUnitInfo;
import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing call of stored procedures
 *
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
@SkipByGridDialect(value = {INFINISPAN,INFINISPAN_REMOTE,NEO4J_EMBEDDED,NEO4J_REMOTE}, comment = "These dialects not support stored procedures")
@TestForIssue( jiraKey = {"OGM-359"})
public class IndexedStoredProcedureCallTest extends OgmJpaTestCase {

	public static final String TEST_SIMPLE_VALUE_STORED_PROC = "testSimpleValue";
	public static final String TEST_RESULT_SET_STORED_PROC = "testResultSet";
	private EntityManager em;

	@Before
	public void setUp() {
		em = getFactory().createEntityManager();
	}

	@After
	public void tearDown() {
		em.close();
	}

	/*@Test
	public void dynamicCallOfStoredProcedureWithoutParametersAndResult() throws Exception {
		// no parameters, no results
		final AtomicInteger result = new AtomicInteger( 1 );
		IndexedStoredProcDialect.FUNCTIONS.put( "testproc1", new IndexedStoredProcedure() {
			@Override
			public ClosableIterator<Tuple> execute(Object[] params) {
				result.set( 10 );
				return null;
			}
		} );


		StoredProcedureQuery call1 = em.createStoredProcedureQuery( "testproc1" );
		assertThat( call1 ).isInstanceOfAny( OgmStoredProcedureQuery.class );
		assertThat( call1.getParameters() ).hasSize( 0 );
		assertThat( call1.execute() ).isEqualTo( false );
		assertThat( result.get() ).isEqualTo( 10 );
	} */

	/**
	 * Testing a call of stored procedure (function) that returns one result
	 * @throws Exception
	 */
	@Test
	public void testSingleResultDynamicCall() throws Exception {
		StoredProcedureQuery storedProcedureQuery = em.createStoredProcedureQuery( TEST_SIMPLE_VALUE_STORED_PROC );
		assertThat( storedProcedureQuery ).isInstanceOfAny( OgmStoredProcedureQuery.class );
		storedProcedureQuery.registerStoredProcedureParameter( 0, Integer.class, ParameterMode.IN );
		storedProcedureQuery.setParameter( 0, 1 );
		Number singleResult = (Number) storedProcedureQuery.getSingleResult();
		assertThat( singleResult ).isNotNull();
		if ( TestHelper.getCurrentDialectType().equals( GridDialectType.MONGODB ) ) {
			//MongoDB can't returns Integer. It returns double :-(
			assertThat( singleResult ).isEqualTo( 1.0d );
		}
		else {
			assertThat( singleResult ).isEqualTo( 1 );
		}
	}
	/**
	 * Testing a  dynamic call of stored procedure (function) that returns cursor.
	 * Transformation to resultset is by mapping class.
	 * @throws Exception
	 */
	@Test
	public void testResultSetDynamicCallWithResultClass() throws Exception {

		StoredProcedureQuery storedProcedureQuery = em.createStoredProcedureQuery( TEST_RESULT_SET_STORED_PROC, Car.class );
		assertThat( storedProcedureQuery ).isInstanceOfAny( OgmStoredProcedureQuery.class );
		storedProcedureQuery.registerStoredProcedureParameter( 0, Void.class, ParameterMode.REF_CURSOR );
		storedProcedureQuery.registerStoredProcedureParameter( 1, String.class, ParameterMode.IN );
		storedProcedureQuery.registerStoredProcedureParameter( 2, String.class, ParameterMode.IN );
		storedProcedureQuery.setParameter( 1, "id" );
		storedProcedureQuery.setParameter( 2, "title" );
		List<Car> listResult = storedProcedureQuery.getResultList();
		assertThat( listResult ).isNotNull();
		assertThat( listResult.size() ).isEqualTo( 1 );
		assertThat( listResult.get( 0 ) ).isEqualTo( new Car( "id", "title" ) );
	}
	/**
	 * Testing a  dynamic call of stored procedure (function) that returns cursor.
	 * Transformation to resultset is by result set mapping.
	 * @throws Exception
	 */

	@Test
	public void testResultSetDynamicCallWithResultMapping() throws Exception {

		StoredProcedureQuery storedProcedureQuery = em.createStoredProcedureQuery( TEST_RESULT_SET_STORED_PROC, "carMapping" );
		assertThat( storedProcedureQuery ).isInstanceOfAny( OgmStoredProcedureQuery.class );
		storedProcedureQuery.registerStoredProcedureParameter( 0,Void.class, ParameterMode.REF_CURSOR );
		storedProcedureQuery.registerStoredProcedureParameter( 1, String.class, ParameterMode.IN );
		storedProcedureQuery.registerStoredProcedureParameter( 2, String.class, ParameterMode.IN );
		storedProcedureQuery.setParameter( 1, "id1" );
		storedProcedureQuery.setParameter( 2, "title'1" );
		List<Car> listResult = storedProcedureQuery.getResultList();
		assertThat( listResult ).isNotNull();
		assertThat( listResult.size() ).isEqualTo( 1 );
		assertThat( listResult.get( 0 ) ).isEqualTo( new Car( "id1","title'1" ) );
	}

	/**
	 * Testing a  static call of stored procedure (function) that returns cursor.
	 * Transformation to resultset is by result class.
	 * @throws Exception
	 */
	@Test
	public void testResultSetStaticCallWithResultClass() throws Exception {
		StoredProcedureQuery storedProcedureQuery = em.createNamedStoredProcedureQuery( "testproc4_1" );
		assertThat( storedProcedureQuery ).isInstanceOfAny( OgmStoredProcedureQuery.class );
		storedProcedureQuery.setParameter( 1, "id" );
		storedProcedureQuery.setParameter( 2, "title" );
		List<Car> listResult = storedProcedureQuery.getResultList();
		assertThat( listResult ).isNotNull();
		assertThat( listResult.size() ).isEqualTo( 1 );
		assertThat( listResult.get( 0 ) ).isEqualTo( new Car( "id", "title" ) );
	}

	/**
	 * Testing a  static call of stored procedure (function) that returns cursor.
	 * Transformation to resultset is by result set mapping.
	 * @throws Exception
	 */
	public void testResultSetStaticCallWithResultMapping() throws Exception {
		StoredProcedureQuery storedProcedureQuery = em.createNamedStoredProcedureQuery( "testproc4_2" );
		assertThat( storedProcedureQuery ).isInstanceOfAny( OgmStoredProcedureQuery.class );
		storedProcedureQuery.setParameter( 1, "id2" );
		storedProcedureQuery.setParameter( 2, "title'2" );
		List<Car> listResult  = storedProcedureQuery.getResultList();
		assertThat( listResult ).isNotNull();
		assertThat( listResult.size() ).isEqualTo( 1 );
		assertThat( listResult.get( 0 ) ).isEqualTo( new Car( "id2", "title'2" ) );
	}

	@Override
	protected void configure(GetterPersistenceUnitInfo info) {

		Properties properties = info.getProperties();

		if ( TestHelper.getCurrentDialectType().equals( GridDialectType.HASHMAP ) ) {
			properties.setProperty( OgmProperties.DATASTORE_PROVIDER, IndexedStoredProcProvider.class.getName() );
		}
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[] { Car.class };
	}
}
