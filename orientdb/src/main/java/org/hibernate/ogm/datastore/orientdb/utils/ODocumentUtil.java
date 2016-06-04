/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class for working with OrientDB's class {@link ODocument}
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class ODocumentUtil {

	/**
	 * Prepare Map with keys of complex names like 'field1.field2'
	 *
	 * @param rootFieldName root name
	 * @param document document
	 * @return Map that contains keys with complex names
	 */
	public static Map<String, Object> extractNamesTree(String rootFieldName, ODocument document) {
		Map<String, Object> map = new LinkedHashMap<>();
		for ( int i = 0; i < document.fields(); i++ ) {
			String fieldName = rootFieldName.concat( "." ).concat( document.fieldNames()[i] );
			Object fieldValue = document.fieldValues()[i];
			if ( fieldValue instanceof ODocument ) {
				map.putAll( extractNamesTree( fieldName, (ODocument) fieldValue ) );
			}
			else {
				map.put( fieldName, fieldValue );
			}
		}
		return map;
	}
}
