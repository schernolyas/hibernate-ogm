/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.orientdb.OrientDBProperties;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;

public class PropertyReaderUtil {

	public static String readHostProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OgmProperties.HOST, String.class ).withDefault( "localhost" ).getValue();
	}

	public static String readDatabaseProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OgmProperties.DATABASE, String.class ).getValue();
	}

	public static String readDatabasePathProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OrientDBProperties.PLOCAL_PATH, String.class ).withDefault( "./target/" ).getValue();
	}

	public static String readRootUserProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OrientDBProperties.ROOT_USERNAME, String.class ).withDefault( "root" ).getValue();
	}

	public static String readRootPasswordProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OrientDBProperties.ROOT_PASSWORD, String.class ).withDefault( "root" ).getValue();
	}

	public static String readUserProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OrientDBProperties.USERNAME, String.class ).getValue();
	}

	public static String readPasswordProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OrientDBProperties.PASSWORD, String.class ).getValue();
	}

	public static Integer readPoolSizeProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OrientDBProperties.POOL_SIZE, Integer.class ).withDefault( 10 ).getValue();
	}

	public static Boolean readCreateDatabaseProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( OgmProperties.CREATE_DATABASE, Boolean.class ).withDefault( Boolean.FALSE ).getValue();
	}

	public static OrientDBProperties.StorageModeEnum readStorateModeProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader
				.property( OrientDBProperties.STORAGE_MODE_TYPE, OrientDBProperties.StorageModeEnum.class )
				.withDefault( OrientDBProperties.StorageModeEnum.MEMORY ).getValue();
	}

	public static OrientDBProperties.DatabaseTypeEnum readDatabaseTypeProperty(ConfigurationPropertyReader propertyReader) {
		return propertyReader
				.property( OrientDBProperties.DATEBASE_TYPE, OrientDBProperties.DatabaseTypeEnum.class )
				.withDefault( OrientDBProperties.DatabaseTypeEnum.DOCUMENT ).getValue();
	}

}
