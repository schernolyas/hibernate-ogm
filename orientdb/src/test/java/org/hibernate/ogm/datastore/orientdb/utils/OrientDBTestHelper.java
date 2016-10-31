/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.orientdb.OrientDB;
import org.hibernate.ogm.datastore.orientdb.OrientDBDialect;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties;
import org.hibernate.ogm.datastore.orientdb.constant.OrientDBConstant;
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

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
		ODatabaseDocumentTx db = provider.getConnection();
		long result = 0;
		Map<String, ClassMetadata> meta = sessionFactory.getAllClassMetadata();

		List<ODocument> docs = NativeQueryUtil.executeIdempotentQuery( db, COUNT_QUERY );
		for ( ODocument doc : docs ) {
			String className = doc.field( "class", String.class );
			ClassMetadata metadata = searchMetadata( meta, className );
			if ( metadata != null ) {
				Long l = doc.field( "c", Long.class );
				result += l;
			}
		}
		return result;

	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		ODatabaseDocumentTx db = provider.getConnection();
		long result = 0;
		Map<String, ClassMetadata> meta = sessionFactory.getAllClassMetadata();

		List<ODocument> docs = NativeQueryUtil.executeIdempotentQuery( db, COUNT_QUERY );
		for ( ODocument doc : docs ) {
			String className = doc.field( "class", String.class );
			ClassMetadata metadata = searchMetadata( meta, className );
			if ( metadata != null ) {
				Long l = doc.field( "c", Long.class );
				result += l;
			}
		}
		return result;

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

	@Override
	public void prepareDatabase(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		ConfigurationPropertyReader propertyReader = provider.getPropertyReader();
		ODatabaseDocumentTx db = provider.getConnection();
		log.infof( "call prepareDatabase! db closed: %s ", db.isClosed() );
		NativeQueryUtil.executeNonIdempotentQuery( db, "ALTER DATABASE TIMEZONE UTC" );
		NativeQueryUtil.executeNonIdempotentQuery( db, "ALTER DATABASE DATEFORMAT '"
				.concat( propertyReader.property( OrientDBProperties.DATE_FORMAT, String.class ).withDefault( "yyyy-MM-dd" ).getValue() ).concat( "'" ) );
		NativeQueryUtil.executeNonIdempotentQuery( db, "ALTER DATABASE DATETIMEFORMAT '"
				.concat( propertyReader.property( OrientDBProperties.DATETIME_FORMAT, String.class ).withDefault( "yyyy-MM-dd HH:mm:ss" ).getValue() )
				.concat( "'" ) );

	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
		OrientDBDatastoreProvider provider = getProvider( sessionFactory );
		ConfigurationPropertyReader propertyReader = provider.getPropertyReader();
		OrientDBProperties.DatabaseTypeEnum databaseType = propertyReader
				.property( OrientDBProperties.DATEBASE_TYPE, OrientDBProperties.DatabaseTypeEnum.class )
				.withDefault( OrientDBProperties.DatabaseTypeEnum.MEMORY ).getValue();
		ODatabaseDocumentTx db = provider.getConnection();
		log.infof( "call dropSchemaAndDatabase! db closed: %b ", db.isClosed() );

		Set<String> docClassSet = new HashSet<>();
		docClassSet.add( "sequences" );

		// remove all documents and classes
		log.info( "try to delete all documents" );
		List<ODocument> result = NativeQueryUtil.executeIdempotentQuery( db, "select from V" );
		for ( ODocument doc : result ) {
			log.infof( "doc: %s ", doc.toJSON() );
			log.infof( "rid class: %s ", doc.field( "@rid" ).getClass().getName() );
			log.infof( "class class: %s ", doc.field( "@class" ).getClass().getName() );
			String docClass = doc.field( "@class", String.class );
			ORecordId rid = doc.field( "@rid", ORecordId.class );
			docClassSet.add( docClass );
			db.delete( rid );
		}
		log.infof( "try to delete classes %s", docClassSet );
		for ( String docClass : docClassSet ) {
			NativeQueryUtil.executeIdempotentQuery( db, String.format( "select executeQuery('drop class %s')", docClass ) );
		}
		// remove all functions
		log.info( "try to delete all functions" );
		for ( ODocument func : NativeQueryUtil.executeIdempotentQuery( db, "select from OFunction" ) ) {
			ORecordId rid = func.field( "@rid", ORecordId.class );
			log.infof( "delete function by rid %s", rid.toString() );
			db.delete( rid );
		}
		String user = propertyReader.property( OgmProperties.USERNAME, String.class ).getValue();
		String password = propertyReader.property( OgmProperties.PASSWORD, String.class ).getValue();
		String host = propertyReader.property( OgmProperties.HOST, String.class ).withDefault( "localhost" ).getValue();
		String database = propertyReader.property( OgmProperties.DATABASE, String.class ).getValue();
		if ( OrientDBProperties.DatabaseTypeEnum.REMOTE.equals( databaseType ) ) {
			OServerAdmin serverAdmin = null;
			try {
				serverAdmin = new OServerAdmin( "remote:" + host ).connect( user, password );
				serverAdmin.dropDatabase( database, OrientDBConstant.PLOCAL_STORAGE_TYPE );
			}
			catch (IOException ioe) {
				log.error( "Canot drop database", ioe );
			}
			finally {
				if ( serverAdmin != null ) {
					serverAdmin.close( true );
				}

			}
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
