/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;

/**
 * Utility class for working with {@link PreparedStatement}
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class PreparedStatementUtil {

	private static final Log log = LoggerFactory.getLogger();

	/**
	 * Set parameter's value of statement
	 *
	 * @param pstmt prepared statement
	 * @param preparedStatementParams list of values
	 */
	public static void setParameters(PreparedStatement pstmt, List<Object> preparedStatementParams) {
		for ( int i = 0; i < preparedStatementParams.size(); i++ ) {
			Object value = preparedStatementParams.get( i );
			try {
				if ( value instanceof byte[] ) {
					pstmt.setBytes( i + 1, (byte[]) value );
				}
				else {
					pstmt.setObject( i + 1, value );
				}
			}
			catch (SQLException sqle) {
				throw log.cannotSetValueForParameter( i + 1, sqle );
			}
		}
	}

}
