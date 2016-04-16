/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.impl;

import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.hibernate.datastore.ogm.orientdb.OrientDBDialect;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.MemoryDBUtil;
import org.hibernate.engine.jndi.spi.JndiService;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.SchemaDefiner;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.ogm.util.configurationreader.spi.PropertyReaderContext;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.hibernate.datastore.ogm.orientdb.transaction.impl.OrientDbTransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBDatastoreProvider extends BaseDatastoreProvider
implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {
    private final ThreadLocal<Connection> connectionHolder = new ThreadLocal<Connection>() {
        private Connection connection;

        @Override
        protected Connection initialValue() {
            try {
                log.debugf("create connection for thread %s", Thread.currentThread().getName());
            connection = DriverManager.getConnection( jdbcUrl, info );
            connection.setAutoCommit( false );
	    setDateFormats( connection );
            } catch (SQLException sqle) {
                    throw log.cannotCreateConnection(sqle);
                }
            return connection; 
        }
        
        @Override
        public Connection get() {
             log.debugf("get connection for thread %s", Thread.currentThread().getName());
             if (connection==null) {
                 connection = initialValue();
             }
             OrientJdbcConnection oc = (OrientJdbcConnection) connection;
             if (oc.getDatabase().getTransaction() instanceof OTransactionNoTx) {
                 log.debug("no transaction");
             } else {
                 log.debugf("transaction: %s",oc.getDatabase().getTransaction());
                 
             } 
            return connection; 
        }
    };

	private static boolean isInmemoryDB = false;
	private static Log log = LoggerFactory.getLogger();
	private static OrientGraphFactory factory;
	private ConfigurationPropertyReader propertyReader;
	private ServiceRegistryImplementor registry;
	private JtaPlatform jtaPlatform;
	private JndiService jndiService;
        private  String jdbcUrl;
        private Properties info;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return OrientDBDialect.class;
	}

	@Override
	public void start() {
		log.debug( "start" );
		try {
			PropertyReaderContext<String> jdbcUrlPropery = propertyReader.property( "javax.persistence.jdbc.url", String.class );
			if ( jdbcUrlPropery != null ) {
				jdbcUrl = jdbcUrlPropery.getValue();
				log.warn( "jdbcUrl:" + jdbcUrl );
				Class.forName( propertyReader.property( "javax.persistence.jdbc.driver", String.class ).getValue() ).newInstance();
				info = new Properties();
				info.put( "user", propertyReader.property( "javax.persistence.jdbc.user", String.class ).getValue() );
				info.put( "password", propertyReader.property( "javax.persistence.jdbc.password", String.class ).getValue() );
				createInMemoryDB();
			}
                        
		}
		catch (Exception e) {
			throw log.unableToStartDatastoreProvider( e );
		}
	}

	private static void setDateFormats(Connection connection) {
		String[] queries = new String[]{ "ALTER DATABASE DATETIMEFORMAT \"" + OrientDBConstant.DATETIME_FORMAT + "\"",
				"ALTER DATABASE DATEFORMAT \"" + OrientDBConstant.DATE_FORMAT + "\"" };
		for ( String query : queries ) {
			try {
				connection.createStatement().execute( query );
			}
			catch (SQLException sqle) {
				throw log.cannotExecuteQuery( query, sqle );
			}
		}
	}

	private void createInMemoryDB() {

		String orientDbUrl = propertyReader.property( "javax.persistence.jdbc.url", String.class ).getValue().substring( "jdbc:orient:".length() );
		Boolean restart = propertyReader.property( "restart.inmemorydb", Boolean.class ).withDefault( Boolean.FALSE ).getValue();

		if ( orientDbUrl.startsWith( "memory" ) ) {
			isInmemoryDB = true;
			log.debugf( "in-memory database exists: %b ",
					( MemoryDBUtil.getOrientGraphFactory() != null ? MemoryDBUtil.getOrientGraphFactory().exists() : false ) );
			if ( ( MemoryDBUtil.getOrientGraphFactory() != null ? MemoryDBUtil.getOrientGraphFactory().exists() : false ) && restart ) {
				log.debugf( "try to recreate in-memory database %s", orientDbUrl );
				MemoryDBUtil.getOrientGraphFactory().close();
				MemoryDBUtil.getOrientGraphFactory().drop();
			}
			MemoryDBUtil.createDbFactory( orientDbUrl );
		}

	}

	public Connection getConnection()  {
                return connectionHolder.get();                
	}

	

	@Override
	public void stop() {

		log.debug( "stop" );
		if ( MemoryDBUtil.getOrientGraphFactory() != null ) {
			if ( MemoryDBUtil.getOrientGraphFactory().exists() ) {
				MemoryDBUtil.getOrientGraphFactory().close();
				MemoryDBUtil.getOrientGraphFactory().drop();
			}
		}
	}

	@Override
	public void configure(Map cfg) {
		log.debugf( "config map: %s" ,cfg.toString() );
		propertyReader = new ConfigurationPropertyReader( cfg );

	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
                log.debug( "injectServices");
		this.registry = serviceRegistry;
		jtaPlatform = serviceRegistry.getService( JtaPlatform.class );
		jndiService = serviceRegistry.getService( JndiService.class );
	}

	@Override
	public Class<? extends SchemaDefiner> getSchemaDefinerType() {
		return OrientDBSchemaDefiner.class;
	}
        
        @Override
	public TransactionCoordinatorBuilder getTransactionCoordinatorBuilder(TransactionCoordinatorBuilder coordinatorBuilder) {
		return new OrientDbTransactionCoordinatorBuilder( coordinatorBuilder, this );
	}
}        
