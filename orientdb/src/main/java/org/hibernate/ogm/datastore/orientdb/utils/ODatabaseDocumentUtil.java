/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import java.sql.Connection;

import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

public class ODatabaseDocumentUtil {
	
	public static ODatabaseDocumentTx getDatabase(Connection connection) {
		ODatabaseDocumentTx db = (ODatabaseDocumentTx) ((OrientJdbcConnection)connection).getDatabase();
		return db;		
	}

}
