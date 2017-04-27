/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.utils.test.secondaryindex;

import java.util.Set;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.ogm.datastore.ignite.IgniteDialect;
import org.hibernate.ogm.datastore.ignite.impl.IgniteCacheInitializer;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.ignite.utils.IgniteTestHelper;
import org.hibernate.ogm.utils.OgmTestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 */

public class ReadThroughSecondaryIndexTest extends OgmTestCase {
	private Log logger = LoggerFactory.getLogger();
	private final ReadThroughFruit apple = new ReadThroughFruit( "1", "apple", "apple desc" );
	private final ReadThroughFruit plum = new ReadThroughFruit( "2", "plum", "plum desc" );
	private final ReadThroughFruit pear = new ReadThroughFruit( "3", "pear", "pear desc" );

	@Before
	public void init() {
		Session session = openSession();
		Transaction transaction = session.beginTransaction();
		session.persist( apple );
		session.persist( plum );
		session.persist( pear );
		transaction.commit();
		session.clear();
		session.close();
		IgniteTestHelper.getProvider( sessionFactory ).clearCache( "ReadThroughFruit" );
	}

	@After
	public void tearDown() {
		Session session = openSession();
		Transaction tx = session.beginTransaction();
		delete( session, apple );
		delete( session, plum );
		delete( session, pear );
		tx.commit();
		session.clear();
		session.close();
		IgniteTestHelper.getProvider( sessionFactory ).clearCache( "ReadThroughFruit" );
	}

	private void delete(Session session, ReadThroughFruit fruit) {
		Object entity = session.get( ReadThroughFruit.class, fruit.getId() );
		if ( entity != null ) {
			session.delete( entity );
		}
	}

	@Test
	public void testIndexes() throws Exception {

		IgniteCache<Object, BinaryObject> indexCache =
				IgniteTestHelper.getProvider( sessionFactory ).getEntityCache( IgniteCacheInitializer.generateIndexName(
						"ReadThroughFruit",
						"title" ) );
		assertThat( indexCache.containsKey( apple.getTitle() ) ).isEqualTo( true );
		assertThat( indexCache.containsKey( plum.getTitle() ) ).isEqualTo( true );
		assertThat( indexCache.containsKey( pear.getTitle() ) ).isEqualTo( true );

		assertThat( indexCache.get( apple.getTitle() ).hasField( IgniteDialect.INDEX_FIELD ) ).isEqualTo( true );
		assertThat( indexCache.get( plum.getTitle() ).hasField( IgniteDialect.INDEX_FIELD ) ).isEqualTo( true );
		assertThat( indexCache.get( pear.getTitle() ).hasField( IgniteDialect.INDEX_FIELD ) ).isEqualTo( true );

		for ( ReadThroughFruit fruit : new ReadThroughFruit[] { apple, pear, plum } ) {
			Set fruitIndex = indexCache.get( fruit.getTitle() ).field( IgniteDialect.INDEX_FIELD );
			assertThat( fruitIndex.size() ).isEqualTo( 1 );
			assertThat( fruitIndex ).contains( fruit.getId() );
		}

		//add new apple

		ReadThroughFruit apple2 = new ReadThroughFruit( "4", "apple", "apple2 desc" );

		Session session = openSession();
		Transaction transaction = session.beginTransaction();
		session.persist( apple2 );
		transaction.commit();
		session.clear();
		session.close();

		Set appleIndex = indexCache.get( apple2.getTitle() ).field( IgniteDialect.INDEX_FIELD );
		assertThat( appleIndex.size() ).isEqualTo( 2 );
		assertThat( appleIndex ).contains( apple2.getId(), apple.getId() );

		//remove old value (remove cache)
		session = openSession();
		transaction = session.beginTransaction();
		session.delete( plum );
		transaction.commit();
		session.clear();
		session.close();

		assertThat( indexCache.containsKey( plum.getTitle() ) ).isEqualTo( true );
		assertThat( indexCache.get( plum.getTitle() ).hasField( IgniteDialect.INDEX_FIELD ) ).isEqualTo( true );
		Set plumIndex = indexCache.get( plum.getTitle() ).field( IgniteDialect.INDEX_FIELD );
		assertThat( plumIndex.size() ).isEqualTo( 0 );
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[] { ReadThroughFruit.class };
	}
}
