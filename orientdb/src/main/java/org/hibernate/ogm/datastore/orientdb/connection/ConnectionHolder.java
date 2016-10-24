/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.connection;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;

/**
 * The class is thread local connection holder.
 * <p>
 * OrientDB uses paradigm "one thread-&gt; one transaction-&gt; one database connection". For implement it, Hibernate
 * OGM uses thread local class for hold connection for each thread (and each transaction). Each thread get part in
 * <b>only one transaction</b>.
 * </p>
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 * @see <a href="http://orientdb.com/docs/2.2/Concurrency.html">Concurrency in OrientDB</a>
 * @see <a href="http://orientdb.com/docs/2.2/Transactions.html">Transactions in OrientDB</a>
 * @see <a href="http://orientdb.com/docs/2.2/Java-Multi-Threading.html">Multi-Threading in OrientDB</a>
 */
public class ConnectionHolder extends ThreadLocal<ODatabaseDocumentTx> {

	private static Log log = LoggerFactory.getLogger();
	private final String orientDbUrl;
	private final String user;
	private final String password;
	
	private final OPartitionedDatabasePoolFactory factory = new OPartitionedDatabasePoolFactory( 10 );

	

	public ConnectionHolder(String orientDbUrl, String user, String password) {
		super();
		this.orientDbUrl = orientDbUrl;
		this.user = user;
		this.password = password;
	}

	@Override
	protected ODatabaseDocumentTx initialValue() {
		log.debugf( "create connection %s for thread %s", orientDbUrl, Thread.currentThread().getName() );
		return createConnectionForCurrentThread();
	}

	@Override
	public void remove() {
		log.debugf( "remove connection for thread %s", Thread.currentThread().getName() );
		try {
			get().close();
		}
		catch ( OException oe) {
			log.error( "Cannot close connection", oe );
		}
		super.remove();
	}

	private ODatabaseDocumentTx createConnectionForCurrentThread() {		
		OPartitionedDatabasePool pool = factory.get("url","user","password");
		return pool.acquire();
	}

}
