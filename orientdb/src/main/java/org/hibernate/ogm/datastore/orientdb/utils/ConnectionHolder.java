/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;

/**
 * The class is connection holder.
 * <p>
 * OrientDB uses paradigm "one thread-&gt; one transaction-&gt; one database connection". In this way, client that
 * supports ACID have to support the paradigm. For it uses thread local class for hold connection for each thread.
 * </p>
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 * @see <a href="http://orientdb.com/docs/2.2/Concurrency.html">Concurrency in OrientDB</a>
 * @see <a href="http://orientdb.com/docs/2.2/Transactions.html">Transactions in OrientDB</a>
 * @see <a href="http://orientdb.com/docs/2.2/Java-Multi-Threading.html">Multi-Threading in OrientDB</a>
 */
public class ConnectionHolder extends ThreadLocal<Connection> {

	private static Log log = LoggerFactory.getLogger();
	private final String jdbcUrl;
	private final Properties jdbcProperties;

	/**
	 * Constructor
	 *
	 * @param jdbcUrl JDBC URL to database
	 * @param info JDBC properties
	 */
	public ConnectionHolder(String jdbcUrl, Properties info) {
		this.jdbcUrl = jdbcUrl;
		this.jdbcProperties = info;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Connection initialValue() {
		log.debugf( "create connection %s for thread %s", jdbcUrl, Thread.currentThread().getName() );
		return createConnectionForCurrentThread();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void remove() {
		log.debugf( "remove connection for thread %s", Thread.currentThread().getName() );
		try {
			get().close();
		}
		catch (SQLException | OException sqle) {
			log.error( "Cannot close connection", sqle );
		}
		super.remove();
	}

	private OrientJdbcConnection createConnectionForCurrentThread() {
		OrientJdbcConnection connection = null;
		try {
			connection = (OrientJdbcConnection) DriverManager.getConnection( jdbcUrl, jdbcProperties );
			connection.setAutoCommit( false );
		}
		catch (SQLException sqle) {
			throw log.cannotCreateConnection( sqle );
		}
		return (OrientJdbcConnection) connection;
	}

}
