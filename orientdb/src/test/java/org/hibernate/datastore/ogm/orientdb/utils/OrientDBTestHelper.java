/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hibernate.SessionFactory;
import org.hibernate.datastore.ogm.orientdb.OrientDB;
import org.hibernate.datastore.ogm.orientdb.OrientDBDialect;
import org.hibernate.datastore.ogm.orientdb.OrientDBSimpleTest;
import org.hibernate.datastore.ogm.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.spi.DatastoreConfiguration;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.spi.TupleSnapshot;
import org.hibernate.ogm.utils.GridDialectOperationContexts;
import org.hibernate.ogm.utils.TestableGridDialect;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBTestHelper implements TestableGridDialect {

	private static final Log log = LoggerFactory.getLogger();
	private static final String COUNT_QUERY = "select @class,count(@rid) as c from V group by @class";

	private static final String JDBC_URL = "jdbc:orient:".concat( OrientDBSimpleTest.MEMORY_TEST );
	private static OrientGraph graph;

	public OrientDBTestHelper() {
		log.info( "call me" );
		// create OrientDB in memory
		graph = MemoryDBUtil.createDbFactory( OrientDBSimpleTest.MEMORY_TEST );

	}

	@Override
	public long getNumberOfEntities(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		OrientJdbcConnection connection = (OrientJdbcConnection) provider.getConnection();
		long result = 0;
		try {
			ResultSet rs = connection.createStatement().executeQuery( COUNT_QUERY );
			while ( rs.next() ) {
				String className = rs.getString( 1 );
				log.debugf( "Vertex class %s. count: %d", className, rs.getLong( 2 ) );
				if ( !className.contains( "_" ) ) {
					result += rs.getLong( 2 );
				}
			}
		}
		catch (SQLException e) {
			log.cannotExecuteQuery( COUNT_QUERY, e );
		}
		return result;
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		OrientJdbcConnection connection = (OrientJdbcConnection) provider.getConnection();
		long result = 0;

		try {
			ResultSet rs = connection.createStatement().executeQuery( COUNT_QUERY );
			while ( rs.next() ) {
				String className = rs.getString( 1 );
				log.debugf( "Vertex class %s. count: %d", className, rs.getLong( 2 ) );
				if ( className.contains( "_" ) ) {
					result += rs.getLong( 2 );
				}
			}
		}
		catch (SQLException e) {
			log.cannotExecuteQuery( COUNT_QUERY, e );
		}
		return result;

	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory, AssociationStorageType type) {
		// return TestHelper.getNumberOfAssociations( sessionFactory, type );
		return 0;

	}

	@Override
	public Map<String, Object> extractEntityTuple(SessionFactory sessionFactory, EntityKey key) {
		Map<String, Object> tuple = new HashMap<>();
		GridDialect dialect = getDialect( sessionFactory );
		TupleSnapshot snapshot = dialect.getTuple( key, GridDialectOperationContexts.emptyTupleContext() ).getSnapshot();
		for ( String column : snapshot.getColumnNames() ) {
			tuple.put( column, snapshot.get( column ) );
		}
		return tuple;
	}

	@Override
	public boolean backendSupportsTransactions() {
		return true;
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
		log.infof( "call dropSchemaAndDatabase! db closed: %b ", graph.isClosed() );
		/*
		 * if ( graphNoTx.countVertices() > 0 ) { MemoryDBUtil.recrateInMemoryDn( OrientDBSimpleTest.MEMORY_TEST ); }
		 */

	}

	@Override
	public Map<String, String> getEnvironmentProperties() {
		return readProperties();
	}

	private static Map<String, String> readProperties() {
		try {
			Properties hibProperties = new Properties();
			hibProperties.load( Thread.currentThread().getContextClassLoader().getResourceAsStream( "hibernate.properties" ) );
			Map<String, String> props = new HashMap<>();
			for ( Map.Entry<Object, Object> entry : hibProperties.entrySet() ) {
				props.put( String.valueOf( entry.getKey() ), String.valueOf( entry.getValue() ) );
				log.info( entry.toString() );
			}
			return Collections.unmodifiableMap( props );
		}
		catch (IOException e) {
			throw new RuntimeException( "Missing properties file: hibernate.properties" );
		}
	}

	@Override
	public Class<? extends DatastoreConfiguration<?>> getDatastoreConfigurationType() {
		return OrientDB.class;
	}

	@Override
	public GridDialect getGridDialect(DatastoreProvider datastoreProvider) {
		return new OrientDBDialect( (OrientDBDatastoreProvider) datastoreProvider );
	}

	private static OrientDBDatastoreProvider getProvider(SessionFactory sessionFactory) {
		DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService( DatastoreProvider.class );
		if ( !( OrientDBDatastoreProvider.class.isInstance( provider ) ) ) {
			throw new RuntimeException( "Not testing with OrientDB, cannot extract underlying provider" );
		}
		return OrientDBDatastoreProvider.class.cast( provider );
	}

	private static GridDialect getDialect(SessionFactory sessionFactory) {
		GridDialect dialect = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService( GridDialect.class );
		return dialect;
	}

}
