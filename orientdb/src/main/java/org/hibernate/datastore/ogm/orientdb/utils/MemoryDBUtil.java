/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph;
import org.jboss.logging.Logger;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class MemoryDBUtil {

	private static final Logger LOG = Logger.getLogger( MemoryDBUtil.class.getName() );
	private static OrientGraphFactory factory;

	public static void dropInMemoryDb(String url) {
                LOG.log(Logger.Level.WARN, "drop database "+url);
		if ( getOrientGraphFactory().exists() ) {
                    	getOrientGraphFactory().drop();
                        LOG.log(Logger.Level.WARN, "database "+url+" droped!");
			getOrientGraphFactory().close();
                        LOG.log(Logger.Level.WARN, "database "+url+" closed!");
		}
		//return createDbFactory( url );
	};

	public static ODatabaseDocumentTx createDbFactory(String url) {
                if (factory!=null && factory.exists() ) {                    
                    return factory.getDatabase();
                }
		factory = new OrientGraphFactory( url );
		// see https://github.com/orientechnologies/orientdb/issues/5688
		factory.setStandardElementConstraints( false );
                factory.setUseLog(true);
                return factory.getDatabase(true, true);
	}

	public static OrientGraphFactory getOrientGraphFactory() {
		return factory;
	}
}
