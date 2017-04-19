/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.utils.test.associations;

import org.hibernate.Session;
import org.hibernate.Transaction;

import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.utils.OgmTestCase;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;
import static org.hibernate.ogm.utils.TestHelper.getNumberOfAssociations;
import static org.hibernate.ogm.utils.TestHelper.getNumberOfEntities;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */
public class ReadWriteThroughManyToOneTest extends OgmTestCase {
	private Log logger = LoggerFactory.getLogger();


	@Test
	public void testUnidirectionalManyToOne() throws Exception {

		final Session session = openSession();
		Transaction transaction = session.beginTransaction();
		CacheStoreJUG jug = new CacheStoreJUG( "summer_camp" );
		jug.setName( "CacheStoreJUG Summer Camp" );
		session.persist( jug );
		CacheStoreMember emmanuel = new CacheStoreMember( "emmanuel" );
		emmanuel.setName( "Emmanuel Bernard" );
		emmanuel.setMemberOf( jug );
		CacheStoreMember jerome = new CacheStoreMember( "jerome" );
		jerome.setName( "Jerome" );
		jerome.setMemberOf( jug );
		session.persist( emmanuel );
		session.persist( jerome );
		session.flush();
		assertThat( getNumberOfEntities( session ) ).isEqualTo( 3 );
		//assertThat( getNumberOfAssociations( session ) ).isEqualTo( expectedAssociations() );
		transaction.commit();
		assertThat( getNumberOfEntities( sessionFactory ) ).isEqualTo( 3 );
		//assertThat( getNumberOfAssociations( sessionFactory ) ).isEqualTo( expectedAssociations() );

		session.clear();
		assertThat( JUGBinaryStore.store.size() ).isEqualTo( 1 );
		logger.debugf( "JUG keys: %s",JUGBinaryStore.store.keySet() );
		assertThat( MemberBinaryStore.store.size() ).isEqualTo( 2 );
		logger.debugf( "Member keys: %s",MemberBinaryStore.store.keySet() );


		transaction = session.beginTransaction();
		emmanuel = session.get( CacheStoreMember.class, emmanuel.getId() );
		jug = emmanuel.getMemberOf();
		session.delete( emmanuel );
		jerome = session.get( CacheStoreMember.class, jerome.getId() );
		session.delete( jerome );
		session.delete( jug );
		transaction.commit();
		assertThat( getNumberOfEntities( sessionFactory ) ).isEqualTo( 0 );
		assertThat( getNumberOfAssociations( sessionFactory ) ).isEqualTo( 0 );

		session.close();

		checkCleanCache();
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] {
				CacheStoreJUG.class, CacheStoreMember.class};
	}
}
