/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.storedprocedures.indexed;

import static org.hibernate.ogm.backendtck.storedprocedures.indexed.IndexedStoredProcedureCallTest.TEST_RESULT_SET_STORED_PROC;
import static org.hibernate.ogm.backendtck.storedprocedures.indexed.IndexedStoredProcedureCallTest.TEST_SIMPLE_VALUE_STORED_PROC;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.util.impl.CollectionHelper;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

/**
 * The provider for testing stored procedures on datastore that supports positional parameters
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class IndexedStoredProcProvider extends BaseDatastoreProvider implements Startable, Stoppable, Configurable,
		ServiceRegistryAwareService {
	private ServiceRegistryImplementor serviceRegistry;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return IndexedStoredProcDialect.class;
	}

	@Override
	public void configure(Map configurationValues) {

	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public void start() {
		// function with one parameter and result as list of entities
		IndexedStoredProcDialect.FUNCTIONS.put( TEST_RESULT_SET_STORED_PROC, new IndexedStoredProcedure() {

			@Override
			public ClosableIterator<Tuple> execute(Object[] params) {
				List<Tuple> result = new ArrayList<>( 1 );
				Tuple resultTuple = new Tuple();
				resultTuple.put( "id", params[0] );
				resultTuple.put( "title", params[1] );
				result.add( resultTuple );
				return CollectionHelper.newClosableIterator( result );
			}
		} );
		// function with one parameter and returned simple value
		IndexedStoredProcDialect.FUNCTIONS.put( TEST_SIMPLE_VALUE_STORED_PROC, new IndexedStoredProcedure() {

			@Override
			public ClosableIterator<Tuple> execute(Object[] params) {
				List<Tuple> result = new ArrayList<>( 1 );
				Tuple resultTuple = new Tuple();
				resultTuple.put( "result", params[0] );
				result.add( resultTuple );
				return CollectionHelper.newClosableIterator( result );
			}
		} );
	}

	@Override
	public void stop() {
		IndexedStoredProcDialect.FUNCTIONS.clear();
	}
}
