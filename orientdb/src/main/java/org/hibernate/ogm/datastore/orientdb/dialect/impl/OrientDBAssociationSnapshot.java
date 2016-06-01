/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.ogm.datastore.orientdb.dialect.impl;

import java.util.Map;
import java.util.Set;

import org.hibernate.ogm.model.key.spi.RowKey;
import org.hibernate.ogm.model.spi.AssociationSnapshot;
import org.hibernate.ogm.model.spi.Tuple;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBAssociationSnapshot implements AssociationSnapshot {

	private final Map<RowKey, Tuple> tuples;

	public OrientDBAssociationSnapshot(Map<RowKey, Tuple> tuples) {
		this.tuples = tuples;
	}

	@Override
	public boolean containsKey(RowKey rowKey) {
		return tuples.containsKey( rowKey );
	}

	@Override
	public Tuple get(RowKey rowKey) {
		return tuples.get( rowKey );
	}

	@Override
	public Set<RowKey> getRowKeys() {
		return tuples.keySet();
	}

	@Override
	public int size() {
		return tuples.size();
	}
}
