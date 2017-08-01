/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.utils;

import java.util.Map;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class TableEntityTypeMappingInfo {
	private static Map<String, Class<?>> tableEntityTypeMapping;

	public static void initMapping(Map<String, Class<?>> mapping) {
		tableEntityTypeMapping = mapping;
	}

	public static Class getEntityClass(String tableName) {
		return tableEntityTypeMapping.get( tableName  );
	}
}
