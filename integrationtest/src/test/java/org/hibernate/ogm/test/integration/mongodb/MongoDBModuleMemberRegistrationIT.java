/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.test.integration.mongodb;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.mongodb.MongoDB;
import org.hibernate.ogm.test.integration.mongodb.errorhandler.TestErrorHandler;

import org.junit.runner.RunWith;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.persistence20.PersistenceDescriptor;
import org.jboss.shrinkwrap.descriptor.api.persistence20.PersistenceUnit;
import org.jboss.shrinkwrap.descriptor.api.persistence20.Properties;
/**
 * Test for the Hibernate OGM module in WildFly using MongoDB
 *
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 * @author Gunnar Morling
 */
@RunWith(Arquillian.class)
public class MongoDBModuleMemberRegistrationIT extends MongoDBModuleMemberRegistrationScenario {

	@Deployment
	public static Archive<?> createTestArchive() {
		return createTestArchiveFromPersistenceXml( persistenceXml() );
	}

	private static PersistenceDescriptor persistenceXml() {
		String host = System.getenv( "MONGODB_HOSTNAME" );
		String port = System.getenv( "MONGODB_PORT" );
		String username = System.getenv( "MONGODB_USERNAME" );
		String password = System.getenv( "MONGODB_PASSWORD" );

		Properties<PersistenceUnit<PersistenceDescriptor>> propertiesContext = Descriptors.create( PersistenceDescriptor.class )
				.version( "2.0" )
				.createPersistenceUnit()
				.name( "primary" )
				.provider( "org.hibernate.ogm.jpa.HibernateOgmPersistence" )
				.getOrCreateProperties();
		if ( isNotNull( host ) ) {
			propertiesContext.createProperty().name( OgmProperties.HOST ).value( host );
		}
		if ( isNotNull( port ) ) {
			propertiesContext.createProperty().name( OgmProperties.PORT ).value( port );
		}
		if ( isNotNull( username ) ) {
			propertiesContext.createProperty().name( OgmProperties.USERNAME ).value( username );
		}
		if ( isNotNull( password ) ) {
			propertiesContext.createProperty().name( OgmProperties.PASSWORD ).value( password );
		}
		return propertiesContext
					.createProperty().name( OgmProperties.DATASTORE_PROVIDER ).value( MongoDB.DATASTORE_PROVIDER_NAME ).up()
					.createProperty().name( OgmProperties.DATABASE ).value( "ogm_test_database" ).up()
					.createProperty().name( OgmProperties.CREATE_DATABASE ).value( "true" ).up()
					.createProperty().name( OgmProperties.ERROR_HANDLER ).value( TestErrorHandler.class.getName() ).up()
					.createProperty().name( "hibernate.search.default.directory_provider" ).value( "ram" ).up()
					.createProperty().name( "wildfly.jpa.hibernate.search.module" ).value( "org.hibernate.search.orm:${module-slot.org.hibernate.search.short-id}" ).up()
				.up().up();
	}
}
