/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.orientdb.OrientDB;
import org.hibernate.ogm.datastore.orientdb.OrientDBDialect;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties;
import org.hibernate.ogm.datastore.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.spi.DatastoreConfiguration;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.spi.TupleSnapshot;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.ogm.utils.GridDialectOperationContexts;
import org.hibernate.ogm.utils.GridDialectTestHelper;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBTestHelper implements GridDialectTestHelper {

	private static final Log log = LoggerFactory.getLogger();
	private static final String COUNT_QUERY = "select @class,count(@rid) as c from V group by @class";

	private ClassMetadata searchMetadata(Map<String, ClassMetadata> meta, String simpleClassName) {
		ClassMetadata metadata = null;
		for ( Map.Entry<String, ClassMetadata> entry : meta.entrySet() ) {
			String currentFullClassName = entry.getKey();
			ClassMetadata currentMetadata = entry.getValue();
			if ( currentFullClassName.toUpperCase().endsWith( ".".concat( simpleClassName.toUpperCase() ) ) ) {
				metadata = currentMetadata;
				break;
			}
		}
		return metadata;
	}
	@Override
	public long getNumberOfEntities(Session session) {
		return 0;
	}

	@Override
	public long getNumberOfAssociations(Session session) {
		return 0;
	}

	@Override
	public long getNumberOfEntities(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		OrientJdbcConnection connection = (OrientJdbcConnection) provider.getConnection();
		long result = 0;
		Map<String, ClassMetadata> meta = sessionFactory.getAllClassMetadata();
		ResultSet rs = null;
		try {
			rs = connection.createStatement().executeQuery( COUNT_QUERY );
			while ( rs.next() ) {
				String className = rs.getString( 1 );
				ClassMetadata metadata = searchMetadata( meta, className );
				if ( metadata != null ) {
					result += rs.getLong( 2 );
				}
			}
			return result;
		}
		catch (SQLException e) {
			throw new HibernateException( e );
		}
		finally {
			close( rs );
		}
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		OrientJdbcConnection connection = (OrientJdbcConnection) provider.getConnection();
		long result = 0;
		Map<String, ClassMetadata> meta = sessionFactory.getAllClassMetadata();
		ResultSet rs = null;
		try {
			rs = connection.createStatement().executeQuery( COUNT_QUERY );
			String className = rs.getString( 1 );
			ClassMetadata metadata = searchMetadata( meta, className );
			if ( metadata == null ) {
				result += rs.getLong( 2 );
			}
			return result;
		}
		catch (SQLException e) {
			throw log.cannotExecuteQuery( COUNT_QUERY, e );
		}
		finally {
			close( rs );
		}
	}

	private void close(ResultSet rs) {
		if ( rs != null ) {
			try {
				rs.close();
			}
			catch (SQLException e) {
				log.error( "Cannot close result set", e );
			}
		}
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory, AssociationStorageType type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Object> extractEntityTuple(Session session, EntityKey key) {
		Map<String, Object> tuple = new HashMap<>();
		GridDialect dialect = getDialect( session.getSessionFactory() );
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

	public void prepareDatabase(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		ConfigurationPropertyReader propertyReader = provider.getPropertyReader();
		OrientJdbcConnection connection = (OrientJdbcConnection) provider.getConnection();
		log.infof( "call prepareDatabase! db closed: %s ", connection.getDatabase().isClosed() );
		try {
			Statement stmt = connection.createStatement();
			stmt.execute( "ALTER DATABASE TIMEZONE UTC" );
			stmt.execute( "ALTER DATABASE DATEFORMAT '"
					.concat( propertyReader.property( OrientDBProperties.DATE_FORMAT, String.class ).withDefault( "yyyy-MM-dd" ).getValue() ).concat( "'" ) );
			stmt.execute( "ALTER DATABASE DATETIMEFORMAT '"
					.concat( propertyReader.property( OrientDBProperties.DATETIME_FORMAT, String.class ).withDefault( "yyyy-MM-dd HH:mm:ss" ).getValue() )
					.concat( "'" ) );
		}
		catch (SQLException | OException sqle) {
			throw log.cannotAlterDatabaseProperties( sqle );
		}
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		OrientJdbcConnection connection = (OrientJdbcConnection) provider.getConnection();
		log.infof( "call dropSchemaAndDatabase! db closed: %b ", connection.getDatabase().isClosed() );
		if ( !connection.getDatabase().isClosed() ) {
			ODatabaseDocumentTx db = (ODatabaseDocumentTx) connection.getDatabase();
			if ( !db.isActiveOnCurrentThread() ) {
				db.activateOnCurrentThread();
			}
			if ( db.getTransaction().isActive() ) {
				db.getTransaction().close();
			}
			MemoryDBUtil.dropInMemoryDb();
			try {
				connection.close();
			}
			catch (SQLException e) {
			}
			log.info( "call dropSchemaAndDatabase! db droped! " );
		}
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
