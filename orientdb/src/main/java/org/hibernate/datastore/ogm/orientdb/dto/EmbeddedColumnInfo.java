/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dto;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class EmbeddedColumnInfo {

	private String className;
	private String propertyName;

	public EmbeddedColumnInfo(String fullPropertyName) {
		int pos = fullPropertyName.indexOf( "." );
		className = fullPropertyName.substring( 0, pos );
		propertyName = fullPropertyName.substring( pos + 1 );
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}

}
