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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.Map;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class QueryUtil {

	private static final Log log = LoggerFactory.getLogger();

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

	public static List<ODocument> executeNativeQuery(ODatabaseDocumentTx db, StringBuilder query) {
		return executeNativeQuery( db, query.toString() );
	}

	public static List<ODocument> executeNativeQuery(ODatabaseDocumentTx db, String query) {
		List<ODocument> result = db.query( new OSQLSynchQuery<ODocument>( query ) );
		return result;
	}

	public static List<ODocument> executeNativeQueryWithParams(ODatabaseDocumentTx db, String query, Map<String, Object> queryParams) {
		OSQLSynchQuery<ODocument> preparedQuery = new OSQLSynchQuery<>( query );
		List<ODocument> result = db.command( preparedQuery ).execute( queryParams );
		return result;
	}
}
