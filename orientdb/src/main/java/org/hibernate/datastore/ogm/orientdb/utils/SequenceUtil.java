/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class SequenceUtil {

	private static final Log log = LoggerFactory.getLogger();

	public static long getSequence(Connection connection, String seqName) {
		long nextValue = 0;
		String query = String.format( "select sequence('%s').next()", seqName );
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery( query );
			if ( rs.next() ) {
				nextValue = rs.getLong( "sequence" );
			}
		}
		catch (SQLException | OException sqle) {
			throw log.cannotExecuteQuery( query, sqle );
		} 
		return nextValue;
	}
}
