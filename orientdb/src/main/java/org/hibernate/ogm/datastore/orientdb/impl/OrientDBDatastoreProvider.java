/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.impl;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.orientdb.OrientDBDialect;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties.DatabaseTypeEnum;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.orientdb.transaction.impl.OrientDbTransactionCoordinatorBuilder;
import org.hibernate.ogm.datastore.orientdb.utils.ConnectionHolder;
import org.hibernate.ogm.datastore.orientdb.utils.FormatterUtil;
import org.hibernate.ogm.datastore.orientdb.utils.MemoryDBUtil;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.SchemaDefiner;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBDatastoreProvider extends BaseDatastoreProvider implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {

	private static Log log = LoggerFactory.getLogger();
	private ConnectionHolder connectionHolder;
	private ConfigurationPropertyReader propertyReader;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return OrientDBDialect.class;
	}

	@Override
	public void start() {
		try {
			OrientDBProperties.DatabaseTypeEnum databaseType = propertyReader
					.property( OrientDBProperties.DATEBASE_TYPE, OrientDBProperties.DatabaseTypeEnum.class )
					.withDefault( OrientDBProperties.DatabaseTypeEnum.MEMORY ).getValue();

			Properties credentials = credentials( propertyReader );
			String url = jdbcUrl( databaseType );
			createDB( url, databaseType );

			connectionHolder = new ConnectionHolder( url, credentials );

			FormatterUtil.setDateFormatter( createFormatter( propertyReader, OrientDBProperties.DATE_FORMAT, "yyyy-MM-dd" ) );
			FormatterUtil.setDateTimeFormatter( createFormatter( propertyReader, OrientDBProperties.DATETIME_FORMAT, "yyyy-MM-dd HH:mm:ss" ) );
		}
		catch (Exception e) {
			throw log.unableToStartDatastoreProvider( e );
		}
	}

	private Properties credentials(ConfigurationPropertyReader propertyReader) {
		String user = propertyReader.property( OgmProperties.USERNAME, String.class ).getValue();
		String password = propertyReader.property( OgmProperties.PASSWORD, String.class ).getValue();
		Properties info = new Properties();
		info.put( "user", user );
		info.put( "password", password );
		return info;
	}

	private ThreadLocal<DateFormat> createFormatter(final ConfigurationPropertyReader propertyReader, final String property, final String defaultFormat) {
		return new ThreadLocal<DateFormat>() {

			@Override
			protected DateFormat initialValue() {
				return new SimpleDateFormat( propertyReader.property( property, String.class ).withDefault( defaultFormat ).getValue() );
			}
		};
	}

	private String jdbcUrl(OrientDBProperties.DatabaseTypeEnum databaseType) {
		String database = propertyReader.property( OgmProperties.DATABASE, String.class ).getValue();
		StringBuilder jdbcUrl = new StringBuilder( 100 );
		jdbcUrl.append( "jdbc:orient:" ).append( databaseType );
		switch ( databaseType ) {
			case MEMORY:
				jdbcUrl.append( ":" ).append( database );
				break;
			case REMOTE:
				String host = propertyReader.property( OgmProperties.HOST, String.class ).withDefault( "localhost" ).getValue();
				jdbcUrl.append( ":" ).append( host ).append( "/" ).append( database );
				break;
			default:
				throw new UnsupportedOperationException( String.format( "Database type %s unsupported!", databaseType ) );
		}
		return jdbcUrl.toString();
	}

	private void createDB(String jdbcUrl, DatabaseTypeEnum databaseType) {
		if ( databaseType == OrientDBProperties.DatabaseTypeEnum.MEMORY ) {
			if ( MemoryDBUtil.getOrientGraphFactory() != null ) {
				log.debugf( "getCreatedInstancesInPool: %d", MemoryDBUtil.getOrientGraphFactory().getCreatedInstancesInPool() );
			}
			MemoryDBUtil.createDbFactory( jdbcUrl.substring( jdbcUrl.indexOf( OrientDBProperties.DatabaseTypeEnum.MEMORY.name() ) ) );
		}
	}

	public Connection getConnection() {
		return connectionHolder.get();
	}

	public void closeConnection() {
		connectionHolder.remove();
	}

	@Override
	public void stop() {
		log.debug( "---stop---" );
	}

	@Override
	public void configure(Map cfg) {
		log.debugf( "config map: %s", cfg.toString() );
		propertyReader = new ConfigurationPropertyReader( cfg );
	}

	public ConfigurationPropertyReader getPropertyReader() {
		return propertyReader;
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
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
