/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

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

	public static Number getSequence(Connection connection, String seqName) {
		Number nextValue = null;
		String query = String.format( "select sequence('%s').next()", seqName );
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery( query );
			if ( rs.next() ) {
				nextValue = rs.getLong( "sequence" );
				if ( nextValue.intValue() == 0 ) {
					// @TODO fixed in 2.2.0-rc1. see https://github.com/orientechnologies/orientdb/issues/5820
					throw log.cannotExecuteQuery( query, new SQLException( String.format( "Sequence %s not found in the database!", seqName ) ) );
				}
			}
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( query, sqle );
		}
		return nextValue;
	}
}
