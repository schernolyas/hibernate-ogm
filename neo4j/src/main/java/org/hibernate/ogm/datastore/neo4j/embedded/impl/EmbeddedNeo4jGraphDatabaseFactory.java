/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.embedded.impl;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hibernate.ogm.datastore.neo4j.Neo4jProperties;
import org.hibernate.ogm.datastore.neo4j.logging.impl.Log;
import org.hibernate.ogm.datastore.neo4j.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.neo4j.spi.GraphDatabaseServiceFactory;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * Contains methods to create a {@link GraphDatabaseService} for the embedded Neo4j.
 *
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 */
public class EmbeddedNeo4jGraphDatabaseFactory implements GraphDatabaseServiceFactory {
	public static final int WAITING_TIME_MS = 100;

	private static final Map<String, FactoryHolder> GRAPH_DATABASE_SERVICE_MAP = new ConcurrentHashMap<>();
	private static Log LOG = LoggerFactory.make( MethodHandles.lookup() );

	private File dbLocation;

	private URL configurationLocation;

	private Map<?, ?> configuration;

	@Override
	public void initialize(Map<?, ?> properties) {
		ConfigurationPropertyReader configurationPropertyReader = new ConfigurationPropertyReader( properties );

		String path = configurationPropertyReader.property( Neo4jProperties.DATABASE_PATH, String.class )
				.required()
				.getValue();

		this.dbLocation = new File( path );

		this.configurationLocation = configurationPropertyReader
				.property( Neo4jProperties.CONFIGURATION_RESOURCE_NAME, URL.class )
				.getValue();

		configuration = properties;
	}

	@Override
	public GraphDatabaseService create() {
		final String dbLocationAbsolutePath = dbLocation.getAbsolutePath();
		AtomicBoolean isNew = new AtomicBoolean( false );
		FactoryHolder factoryHolder = GRAPH_DATABASE_SERVICE_MAP.computeIfAbsent( dbLocationAbsolutePath, ( String dbPath ) -> {
			LOG.infof( " Create new service instance for dbPath  %s", dbLocationAbsolutePath );
			GraphDatabaseFacade service = buildGraphDatabaseService();
			FactoryHolder holder = new FactoryHolder();
			holder.setCounter( 1 );
			holder.setGraphDatabaseFacade( service );
			isNew.set( true );
			return holder;
		} );
		if ( !isNew.get() ) {
			synchronized (GRAPH_DATABASE_SERVICE_MAP) {
				factoryHolder.getGraphDatabaseFacade();
				boolean isAvailable = factoryHolder.getGraphDatabaseFacade().isAvailable( WAITING_TIME_MS );
				if ( isAvailable ) {
					factoryHolder.setCounter( factoryHolder.getCounter() + 1 );
				}
				else {
					//need recreate db
					LOG.infof( " Recreate service instance for dbPath  %s", dbLocationAbsolutePath );
					factoryHolder.setCounter( 1 );
					factoryHolder.setGraphDatabaseFacade( buildGraphDatabaseService() );
				}
				LOG.debugf( " Counter : %s for path %s", factoryHolder.getCounter(), dbLocationAbsolutePath );
			}
		}
		return factoryHolder.getGraphDatabaseFacade();
	}

	private GraphDatabaseFacade buildGraphDatabaseService() {
		GraphDatabaseService service = null;
		GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbLocation );
		setConfigurationFromLocation( builder, configurationLocation );
		setConfigurationFromProperties( builder, configuration );
		final ClassLoader neo4JClassLoader = builder.getClass().getClassLoader();
		final Thread currentThread = Thread.currentThread();
		final ClassLoader contextClassLoader = currentThread.getContextClassLoader();
		try {
			//Neo4J relies on the context classloader to load its own extensions:
			//Allow it to load even the ones we're not exposing directly to end users.
			currentThread.setContextClassLoader( neo4JClassLoader );
			service =  builder.newGraphDatabase();
			GraphDatabaseFacade sq = (GraphDatabaseFacade) service;
			LOG.debugf( "GraphDatabaseAPI class: %s",sq.getClass().getName() );
			LOG.debugf( "Created new storage at path : %s;  store id: %s",sq.getStoreDir(),sq.storeId() );
		}
		catch (Exception e) {
			throw LOG.cannotCreateNewGraphDatabaseServiceException( e );
		}
		finally {
			currentThread.setContextClassLoader( contextClassLoader );
		}
		return (GraphDatabaseFacade) service;
	}

	private void setConfigurationFromProperties(GraphDatabaseBuilder builder, Map<?, ?> properties) {
		if ( properties != null ) {
			builder.setConfig( convert( properties ) );
		}
	}

	private Map<String, String> convert(Map<?, ?> properties) {
		Map<String, String> neo4jConfiguration = new HashMap<String, String>();
		for ( Map.Entry<?, ?> entry : properties.entrySet() ) {
			neo4jConfiguration.put( String.valueOf( entry.getKey() ), String.valueOf( entry.getValue() ) );
		}
		return neo4jConfiguration;
	}

	private void setConfigurationFromLocation(GraphDatabaseBuilder builder, URL cfgLocation) {
		if ( cfgLocation != null ) {
			builder.loadPropertiesFromURL( cfgLocation );
		}
	}

	static void shutdownGraphDatabaseService(GraphDatabaseService neo4jDb) {
		GraphDatabaseAPI sq = (GraphDatabaseAPI) neo4jDb;
		String key = sq.getStoreDir().getAbsolutePath();
		if ( GRAPH_DATABASE_SERVICE_MAP.containsKey( key ) ) {
			synchronized (GRAPH_DATABASE_SERVICE_MAP) {
				FactoryHolder factoryHolder = GRAPH_DATABASE_SERVICE_MAP.get( key );
				factoryHolder.setCounter( factoryHolder.getCounter() - 1 );
				LOG.debugf( " Counter : %s  for path %s", factoryHolder.getCounter(), key );

				GraphDatabaseFacade graphDatabaseFacade = factoryHolder.getGraphDatabaseFacade();
				try {
					GraphDatabaseFacade.SPI spi = null;
					Object obj = getPrivateField( GraphDatabaseFacade.class.getDeclaredField( "spi" ), graphDatabaseFacade );//
					spi = (GraphDatabaseFacade.SPI) obj;
					LOG.debugf( " isInOpenTransaction: %s", spi.isInOpenTransaction() );
					if ( spi.isInOpenTransaction() ) {
						LOG.debugf( " currentTransaction: %s", spi.currentTransaction() );
						//LOG.warnf( "close transaction %s", spi.currentTransaction().getTransactionId() );
						LOG.warnf( "transaction  started %s", spi.currentTransaction().startTime() );
						spi.currentTransaction().success();
						spi.currentTransaction().closeTransaction();
					}
				}
				catch ( Exception e) {
					LOG.error( "ERROR!", e );
				}

				if ( factoryHolder.getCounter() == 0 ) {
					LOG.debugf( " Shutdown db for path : %s", key );
					factoryHolder.getGraphDatabaseFacade().shutdown();
					GRAPH_DATABASE_SERVICE_MAP.remove( key );
				}
			}
		}
		else {
			throw LOG.unknownDatabasePathException( key );
		}
	}

	private static Object getPrivateField(Field privateField, Object targetObject) {
		Object result = null;
		try {
			//Field privateTransactionField = TopLevelTransaction.class.getDeclaredField( "transaction" );
			privateField.setAccessible( true );
			result =  privateField.get( targetObject );
		}
		catch (Exception e) {
			LOG.error( "ERROR!", e );
		}
		return result;
	}


	private static class FactoryHolder {
		private int counter;
		private GraphDatabaseFacade graphDatabaseFacade;

		int getCounter() {
			return counter;
		}

		void setCounter(int counter) {
			this.counter = counter;
		}

		public GraphDatabaseFacade getGraphDatabaseFacade() {
			return graphDatabaseFacade;
		}

		public void setGraphDatabaseFacade(GraphDatabaseFacade graphDatabaseFacade) {
			this.graphDatabaseFacade = graphDatabaseFacade;
		}
	}

}
