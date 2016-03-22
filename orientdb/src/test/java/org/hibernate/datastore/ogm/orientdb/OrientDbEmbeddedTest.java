/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;

import org.apache.log4j.Logger;
import org.hibernate.datastore.ogm.orientdb.jpa.Car;
import org.hibernate.datastore.ogm.orientdb.jpa.CarOwner;
import org.hibernate.datastore.ogm.orientdb.jpa.EngineInfo;
import org.hibernate.datastore.ogm.orientdb.jpa.Producer;
import org.hibernate.datastore.ogm.orientdb.utils.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDbEmbeddedTest {

	public static final String MEMORY_TEST = "memory:test";
	private static final Logger log = Logger.getLogger( OrientDBSimpleTest.class.getName() );
	private static EntityManager em;
	private static EntityManagerFactory emf;
	private static OrientGraphNoTx graphNoTx;

	@BeforeClass
	public static void setUpClass() {
		graphNoTx = MemoryDBUtil.createDbFactory( MEMORY_TEST );
		emf = Persistence.createEntityManagerFactory( "hibernateOgmJpaUnit" );
		em = emf.createEntityManager();
		em.setFlushMode( FlushModeType.COMMIT );
	}

	@AfterClass
	public static void tearDownClass() {
		if ( em != null ) {
			em.close();
			emf.close();

		}
		graphNoTx.shutdown();
		MemoryDBUtil.recrateInMemoryDn( MEMORY_TEST );
	}

	@Before
	public void setUp() {
		if ( em.getTransaction().isActive() ) {
			em.getTransaction().rollback();
		}
	}

	@After
	public void tearDown() {
		em.clear();
	}

	@Test
	public void test1InsertNewEmbeddedClass() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Producer producer = new Producer();
			producer.setCountry( "RU" );
			producer.setTitle( "Rostech" );
			producer.setPhone( "+74956667788" );

			Car newCar = new Car();
			EngineInfo engineInfo = new EngineInfo();
			engineInfo.setTitle( "E6GV" );
			engineInfo.setCylinders( (short) 6 );
			engineInfo.setPower( 100 );
			engineInfo.setPrice( 1000000 );
			engineInfo.setProducer( producer );
			newCar.setEngineInfo( engineInfo );

			List<CarOwner> owners = new LinkedList<>();
			owners.add( new CarOwner( "name1", true ) );
			owners.add( new CarOwner( "name2", false ) );
			newCar.setOwners( owners );

			em.persist( newCar );
			em.getTransaction().commit();
			em.clear();
			// reread
			em.getTransaction().begin();
			Car car = em.find( Car.class, 1l );
			assertNotNull( "Car must be saved!", car );
			assertNotNull( "Engine info of saved Car must be saved!", car.getEngineInfo() );
			assertEquals( "E6GV", car.getEngineInfo().getTitle() );
			assertNotNull( "Producer of Engine info of saved Car must be saved!", car.getEngineInfo().getProducer() );
			assertEquals( "+74956667788", car.getEngineInfo().getProducer().getPhone() );
			assertNotNull( "Car must have owners!", car.getOwners() );
			assertEquals( "Car must have 2 owners!", 2, car.getOwners().size() );
			for ( CarOwner owner : owners ) {
				assertTrue( "Unkwoun owner :" + owner.getName(),
						( owner.getName().equals( "name1" ) || owner.getName().equals( "name2" ) ) );
				if ( owner.getName().equals( "name1" ) ) {
					assertTrue( "Owner with name1 must be active", owner.isActive() );
				}
				else if ( owner.getName().equals( "name2" ) ) {
					assertFalse( "Owner with name2 must be not active", owner.isActive() );
				}
			}

			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

}
