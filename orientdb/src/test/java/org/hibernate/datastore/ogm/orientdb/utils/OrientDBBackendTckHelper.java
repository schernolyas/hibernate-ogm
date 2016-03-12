/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import static org.hibernate.datastore.ogm.orientdb.OrientDBSimpleTest.MEMORY_TEST;

import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.extensions.cpsuite.ClasspathSuite;
import org.junit.runner.RunWith;

/**
 * Helper class allowing you to run all or any specified subset of test available on the classpath. This method is for
 * example useful to run all or parts of the <i>backendtck</i>.
 *
 * @author Hardy Ferentschik
 */
@RunWith(ClasspathSuite.class)
// @ClasspathSuite.ClassnameFilters({ "org.hibernate.ogm.backendtck.*" })
@ClasspathSuite.ClassnameFilters({ ".*BuiltInTypeTest" })
public class OrientDBBackendTckHelper {

	private static final Logger log = Logger.getLogger( OrientDBBackendTckHelper.class.getName() );

	@BeforeClass
	public static void setUpClass() {
		log.info( "setUpClass" );
		MemoryDBUtil.createDbFactory( MEMORY_TEST );
	}

	@AfterClass
	public static void tearDownClass() {
		log.info( "tearDownClass" );
		MemoryDBUtil.recrateInMemoryDn( MEMORY_TEST );
	}

	@Before
	public void setUp() {
		log.info( "setUp" );
	}

	@After
	public static void tearDown() {
		log.info( "tearDown" );
	}

}