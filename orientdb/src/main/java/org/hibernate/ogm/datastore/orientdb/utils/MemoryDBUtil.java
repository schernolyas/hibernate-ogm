/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class MemoryDBUtil {

	private static OrientGraphFactory factory;

	public static void dropInMemoryDb() {
		if ( getOrientGraphFactory().getDatabase().exists() ) {
			getOrientGraphFactory().getDatabase().drop();
			getOrientGraphFactory().getDatabase().close();
		}
		factory.drop();
		factory = null;
	};

	public static void cleanDbFactory(String url) {
	}

	public static ODatabaseDocumentTx createDbFactory(String url) {
		if ( factory != null ) {
			factory.drop();
			factory = null;
		}
		factory = new OrientGraphFactory( url, true );
		// see https://github.com/orientechnologies/orientdb/issues/5688
		factory.setStandardElementConstraints( false );
		factory.setUseLog( true );
		ODatabaseDocumentTx db = factory.getDatabase( true, true );
		return db;
	}

	public static OrientGraphFactory getOrientGraphFactory() {
		return factory;
	}
}
