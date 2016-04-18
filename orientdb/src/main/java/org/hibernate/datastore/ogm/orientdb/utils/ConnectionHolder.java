/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;

import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class ConnectionHolder extends ThreadLocal<Connection> {

	private static Log log = LoggerFactory.getLogger();
	private final String jdbcUrl;
	private final Properties info;
	private Connection connection;

	public ConnectionHolder(String jdbcUrl, Properties info) {
		this.jdbcUrl = jdbcUrl;
		this.info = info;
	}

	@Override
	protected Connection initialValue() {
		try {
			log.debugf( "create connection for thread %s", Thread.currentThread().getName() );
			connection = DriverManager.getConnection( jdbcUrl, info );
			connection.setAutoCommit( false );
			setDateFormats( connection );
		}
		catch (SQLException sqle) {
			throw log.cannotCreateConnection( sqle );
		}
		return connection;
	}

	@Override
	public Connection get() {
		log.debugf( "get connection for thread %s", Thread.currentThread().getName() );
		if ( connection == null ) {
			connection = initialValue();
		}
		OrientJdbcConnection oc = (OrientJdbcConnection) connection;
		if ( oc.getDatabase().getTransaction() instanceof OTransactionNoTx ) {
			log.debug( "no transaction" );
		}
		else {
			log.debugf( "transaction: %s", oc.getDatabase().getTransaction() );

		}
		return connection;
	}

	private void setDateFormats(Connection connection) {
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

	@Override
	public void remove() {
		try {
			connection.close();
		}
		catch (SQLException e) {
		}
	}

}
