/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.transaction.impl;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import java.sql.Connection;
import org.hibernate.datastore.ogm.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.transaction.impl.ForwardingTransactionCoordinator;
import org.hibernate.ogm.transaction.impl.ForwardingTransactionDriver;
import org.hibernate.resource.transaction.TransactionCoordinator;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class OrientDBLocalTransactionCoordinator extends ForwardingTransactionCoordinator {

	private static Log log = LoggerFactory.getLogger();
	private OrientDBDatastoreProvider datastoreProvider;
	private OTransaction currentOrientDBTransaction;

	public OrientDBLocalTransactionCoordinator(TransactionCoordinator coordinator, OrientDBDatastoreProvider datastoreProvider) {
		super( coordinator );
		this.datastoreProvider = datastoreProvider;
	}

	@Override
	public TransactionDriver getTransactionDriverControl() {
		TransactionDriver driver = super.getTransactionDriverControl();
		return new OrientDBTransactionDriver( driver );
	}

	private void success() {
		if ( currentOrientDBTransaction != null && currentOrientDBTransaction.isActive() ) {
			log.debugf( "commit transaction (Id: %d) for database %s  is %d.",
					currentOrientDBTransaction.getId(),
					currentOrientDBTransaction.getDatabase().getName() );
			currentOrientDBTransaction.commit();
			close();
		}
		else {
			currentOrientDBTransaction = null;
		}
	}

	private void failure() {
		if ( currentOrientDBTransaction != null && currentOrientDBTransaction.isActive() ) {
			log.debugf( "rollback transaction (Id: %d) for database %s  is %d.",
					currentOrientDBTransaction.getId(),
					currentOrientDBTransaction.getDatabase().getName() );
			currentOrientDBTransaction.rollback();
			close();
		}
		else {
			currentOrientDBTransaction = null;
		}
	}

	private void close() {
		try {
			currentOrientDBTransaction.close();
		}
		finally {
			currentOrientDBTransaction = null;
		}
	}

	private class OrientDBTransactionDriver extends ForwardingTransactionDriver {

		public OrientDBTransactionDriver(TransactionDriver delegate) {
			super( delegate );
		}

		@Override
		public void begin() {
			Connection sqlConnection = datastoreProvider.getConnection();
			OrientJdbcConnection orientDbConn = (OrientJdbcConnection) sqlConnection;
			ODatabaseDocumentTx database = orientDbConn.getDatabase();
			log.debugf( "begin transaction for database %s", database.getName() );
			super.begin();
			currentOrientDBTransaction = database.activateOnCurrentThread().begin().getTransaction();
			if ( currentOrientDBTransaction instanceof OTransactionNoTx ) {
				// no active transaction. create it!!
				log.debugf( "no active transactions for database %s . Create new transaction!", database.getName() );
				currentOrientDBTransaction = database.getTransaction();
				log.debugf( "Id of new transaction for database %s  is %d.", database.getName(),
						currentOrientDBTransaction.getId() );
			}
			else {
				log.debugf( "Id of current transaction for database %s  is %d. (transaction: %s)", database.getName(),
						currentOrientDBTransaction.getId(), currentOrientDBTransaction );
				OTransactionOptimistic op = (OTransactionOptimistic) currentOrientDBTransaction;

			}
		}

		@Override
		public void commit() {
			try {
				if ( currentOrientDBTransaction != null && currentOrientDBTransaction.isActive() ) {
					log.debugf( "commit transaction N %s for database %s. Transaction acvite? %s",
							String.valueOf( currentOrientDBTransaction.getId() ),
							currentOrientDBTransaction.getDatabase().getName(),
							String.valueOf( currentOrientDBTransaction.isActive() ) );
					super.commit();
					success();
				}

			}
			catch (Exception e) {
				log.error( "Cannot commit transaction!", e );
				try {
					rollback();
				}
				catch (Exception re) {
				}
				throw e;
			}
		}

		@Override
		public void rollback() {
			try {
				if ( currentOrientDBTransaction != null && currentOrientDBTransaction.isActive() ) {
					log.debugf( "rollback  transaction N %s for database %s. Transaction acvite? %s",
							String.valueOf( currentOrientDBTransaction.getId() ),
							currentOrientDBTransaction.getDatabase().getName(),
							String.valueOf( currentOrientDBTransaction.isActive() ) );
					if ( currentOrientDBTransaction.isActive() ) {
						super.rollback();
					}
				}
			}
			catch (Exception e) {
				log.error( "Cannot rollback transaction!", e );
			}
			finally {
				failure();
			}
		}
	}

}
