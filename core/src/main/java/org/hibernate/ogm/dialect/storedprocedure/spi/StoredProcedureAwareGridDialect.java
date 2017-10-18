/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.dialect.storedprocedure.spi;

import java.util.Map;

import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.dialect.spi.TupleContext;

/**
 * A facet for {@link GridDialect} implementations which support the execution of stored procedures.
 *
 * Cases of stored procedures are :
 * <ou>
 *     <li>procedure without any input or output parameters</li>
 *	   <li>function with many input parameters and one returned value</li>
 * </ou>
 *
 *
 * @see <a href="https://stackoverflow.com/questions/32480060/call-mongodb-function-from-java">Example for MongoDB</a>
 * @see <a href="http://orientdb.com/docs/2.0/orientdb.wiki/Functions.html">Example for OrientDB</a>
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public interface StoredProcedureAwareGridDialect extends GridDialect {

	/**
	 * Is data storage supports parameter position by name (true) or by index (false)
	 *
	 * @return
	 */
	boolean supportsNamedPosition();


	/**
	 * Returns the result of a stored procedure executed on the backend.
	 * Tne method uses for storages that supports name position
	 *
	 * @param storedProcedureName name of stored procedure.
	 * @param params - array with values
	 * @param tupleContext the tuple context
	 *
	 * @return an {@link ClosableIterator} with the result of the query
	 */

	Object callStoredProcedure( String storedProcedureName, Object[] params, TupleContext tupleContext);

	/**
	 * Returns the result of a stored procedure executed on the backend.
	 * Tne method uses for storages that supports name position
	 *
	 * @param storedProcedureName name of stored procedure.
	 * @param params - map with pair "name"-"value"
	 * @param tupleContext the tuple context
	 *
	 * @return an {@link ClosableIterator} with the result of the query
	 */

	Object callStoredProcedure(	String storedProcedureName, Map<String,Object> params, TupleContext tupleContext);

}
