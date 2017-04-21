/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite;

import org.hibernate.ogm.datastore.keyvalue.cfg.KeyValueStoreProperties;

/**
 * Properties for configuring the Ignite datastore
 *
 * @author Dmitriy Kozlov
 *
 */
public final class IgniteProperties implements KeyValueStoreProperties {

	/**
	 * Configuration property for specifying the name of the Ignite configuration file
	 */
	public static final String CONFIGURATION_RESOURCE_NAME = "hibernate.ogm.ignite.configuration_resource_name";
	/**
	 * Configuration property for specifying class name. Class must implements {@link IgniteConfigurationBuilder}
	 */
	public static final String CONFIGURATION_CLASS_NAME = "hibernate.ogm.ignite.configuration_class_name";
	/**
	 * Configuration property for specifying the name existing Ignite instance
	 */
	public static final String IGNITE_INSTANCE_NAME = "hibernate.ogm.ignite.instance_name";

	/**
	 * Configuration property templates for manage read-through and write-through for each entity.
	 * Example of full string "hibernate.ogm.ignite.cache_config.Person.read_through=true"
	 * @see <a href="https://apacheignite.readme.io/docs/persistent-store#read-through-and-write-through">Persistent Store</a>
	 */
	public static final String IGNITE_READ_THROUGH_TEMPLATE = "hibernate.ogm.ignite.cache_config.%s.read_through";
	public static final String IGNITE_WRITE_THROUGH_TEMPLATE = "hibernate.ogm.ignite.cache_config.%s.write_through";
	public static final String IGNITE_STORE_KEEP_BINARY_TEMPLATE = "hibernate.ogm.ignite.cache_config.%s.store_keep_binary";
	public static final String IGNITE_CACHE_STORE_FACTORY_TEMPLATE = "hibernate.ogm.ignite.cache_config.%s.cache_store_factory";

	private IgniteProperties() {

	}

}
