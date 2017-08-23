/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.utils.test.secondaryindex;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.ogm.datastore.ignite.options.CacheStoreFactory;
import org.hibernate.ogm.datastore.ignite.options.ReadThrough;
import org.hibernate.ogm.datastore.ignite.options.StoreKeepBinary;
import org.hibernate.ogm.datastore.ignite.options.WriteThrough;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
@Entity
@ReadThrough
@WriteThrough
@StoreKeepBinary
@CacheStoreFactory(ReadThroughFruitBunaryStore.class)
@Deprecated
public class ReadThroughFruit {
	public static final String TABLE_NAME = "ReadThroughFruit";
	@Id
	private String id;
	private String title;

	private String description;

	public ReadThroughFruit() {
	}

	public ReadThroughFruit(String id, String title, String description) {
		this.id = id;
		this.title = title;
		this.description = description;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( o == null || getClass() != o.getClass() ) {
			return false;
		}
		ReadThroughFruit that = (ReadThroughFruit) o;
		return Objects.equals( id, that.id );
	}

	@Override
	public int hashCode() {
		return Objects.hash( id );
	}
}
