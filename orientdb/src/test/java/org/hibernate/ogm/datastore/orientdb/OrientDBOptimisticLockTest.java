/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import static org.hibernate.ogm.datastore.orientdb.OrientDBSimpleTest.MEMORY_TEST;

import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.OptimisticLockException;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.RollbackException;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;

import org.hibernate.ogm.datastore.orientdb.jpa.Writer;
import org.hibernate.ogm.datastore.orientdb.utils.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * The test is checking optimistic locking for OrientDB
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 * @see <a href="https://blogs.oracle.com/enterprisetechtips/entry/locking_and_concurrency_in_java">Locking and
 * concurrency</a>
 * @see <a href="http://orientdb.com/docs/2.2/Concurrency.html">Concurrency in OrientDB</a>
 * @see <a href="http://orientdb.com/docs/2.2/Transactions.html">Transactions in OrientDB</a>
 * @see <a href="http://orientdb.com/docs/2.2/Java-Multi-Threading.html">Multi-Threading in OrientDB</a>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDBOptimisticLockTest {

	private static final Logger log = Logger.getLogger( OrientDBOptimisticLockTest.class.getName() );
	private static EntityManager em;
	private static EntityManagerFactory emf;

	@BeforeClass
	public static void setUpClass() {
		MemoryDBUtil.createDbFactory( MEMORY_TEST );
		emf = Persistence.createEntityManagerFactory( "hibernateOgmJpaUnit" );
		em = emf.createEntityManager();
		em.setFlushMode( FlushModeType.COMMIT );
	}

	@AfterClass
	public static void tearDownClass() {
		if ( emf != null && em != null ) {
			em.close();
			emf.close();
		}
		MemoryDBUtil.dropInMemoryDb();
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
	public void test1ParallelUpdateEntity() throws Exception {
		Writer writer = null;
		try {
			log.info( "Create first writer" );
			em.getTransaction().begin();
			writer = new Writer();
			writer.setbKey( 1L );
			writer.setCount( 1L );
			writer.setName( "Valter Scott" );
			Calendar calendar = Calendar.getInstance();
			calendar.set( 1771, 11, 15 );
			writer.setBirthDate( calendar.getTime() );
			em.persist( writer );
			em.getTransaction().commit();
			log.info( "Writer persisted" );
			em.clear();
		}
		catch (Exception e) {
			log.error( "Error", e );
			if ( em.getTransaction().isActive() ) {
				em.getTransaction().rollback();
			}
			throw e;
		}

		log.info( "waiting results...." );
		Set<Long> readyForWrite = Collections.synchronizedSet( new HashSet<Long>( 2 ) );
		ForkJoinTask<Long> t1 = ForkJoinPool.commonPool()
				.submit( ForkJoinTask.adapt( new WriterUpdateThread( 2, 1, emf.createEntityManager(), readyForWrite ) ) );
		ForkJoinTask<Long> t2 = ForkJoinPool.commonPool()
				.submit( ForkJoinTask.adapt( new WriterUpdateThread( 3, 1, emf.createEntityManager(), readyForWrite ) ) );

		long t1Result = -1;
		long t2Result = -1;
		try {
			t1Result = t1.get();
			log.info( "t1 result:" + t1Result );
			t2Result = t2.get();
			log.info( "t2 result:" + t2Result );
		}
		catch (ExecutionException e) {
			log.error( "Error in task", e );
			if ( e.getCause() instanceof AssertionError ) {
				throw e;
			}
			RollbackException re = (RollbackException) e.getCause();
			boolean isNotActualVersion = isNotActualVersion( re );
			assertTrue( "Must be right exception (OConcurrentModificationException or HibernateException (id OGM001716). Now: class:"
					+ re.getCause().getClass().getName() + ". Message:" + re.getCause().getMessage(),
					isOConcurrentModificationException( re ) ||
					( isNotActualVersion ) );
		}
		if ( t1.isDone() && t2.isDone() ) {
			if ( isAnyThreadSuccess( t1, t2 ) ) {
				try {
					em.clear();
					em.getTransaction().begin();
					writer = em.find( Writer.class, 1l );
					assertTrue( "Counter must be changed!", writer.getCount() > 1L ); // one thread commited change
					assertEquals( "Name must be uppercase!", "Valter Scott".toUpperCase(), writer.getName() );
					em.getTransaction().commit();
				}
				catch (Exception e) {
					log.error( "Error", e );
					em.getTransaction().rollback();
					throw e;
				}
			}
			else {
				assertTrue( "No success threads!", false );
			}
		}
	}

	@Test
	public void test2ParallelUpdateDeleteEntity() throws Exception {
		Writer writer = null;
		try {
			log.info( "Create second writer" );
			em.getTransaction().begin();
			writer = new Writer();
			writer.setbKey( 2L );
			writer.setCount( 1L );
			writer.setName( "Agniya Barto" );
			Calendar calendar = Calendar.getInstance();
			calendar.set( 1906, 01, 4 );
			writer.setBirthDate( calendar.getTime() );
			em.persist( writer );
			em.getTransaction().commit();
			log.info( "Writer persisted" );
			em.clear();
		}
		catch (Exception e) {
			log.error( "Error", e );
			if ( em.getTransaction().isActive() ) {
				em.getTransaction().rollback();
			}
			throw e;
		}

		log.info( "waiting results...." );
		Set<Long> readyForWrite = Collections.synchronizedSet( new HashSet<Long>( 2 ) );
		ForkJoinTask<Long> t1 = ForkJoinPool.commonPool()
				.submit( ForkJoinTask.adapt( new WriterUpdateThread( 2, 2, emf.createEntityManager(), readyForWrite ) ) );
		ForkJoinTask<Long> t2 = ForkJoinPool.commonPool()
				.submit( ForkJoinTask.adapt( new WriterDeleteThread( 3, 2, emf.createEntityManager(), readyForWrite ) ) );

		long t1Result = -1;
		long t2Result = -1;
		boolean isOrientDBEx = false;
		boolean isOptimisticLockEx = false;
		boolean isNotActualVersion = false;
		try {
			t1Result = t1.get();
			log.info( "t1 result:" + t1Result );
			t2Result = t2.get();
			log.info( "t2 result:" + t2Result );
		}
		catch (ExecutionException e) {
			log.error( "Error in task", e );
			if ( e.getCause() instanceof AssertionError ) {
				throw e;
			}
			RollbackException re = (RollbackException) e.getCause();
			isOrientDBEx = isOConcurrentModificationException( re );
			isOptimisticLockEx = isOptimisticLockException( re );
			isNotActualVersion = isNotActualVersion( re );
			assertTrue(
					"Must be right exception (OConcurrentModificationException or OptimisticLockException or HibernateException (id OGM001716) ).Now: class:"
							+ re.getCause().getClass().getName() + ". Message:" + re.getCause().getMessage(),
							( isOrientDBEx || isOptimisticLockEx || isNotActualVersion ) );
		}
		if ( t1.isDone() && t2.isDone() ) {
			if ( isAnyThreadSuccess( t1, t2 ) ) {
				try {
					log.debug( String.format( "Exception flags: OConcurrentModificationException: %s;OptimisticLockException:%s;HibernateException:%s",
							isOrientDBEx, isOptimisticLockEx, isNotActualVersion ) );
					em.clear();
					em.getTransaction().begin();
					writer = em.find( Writer.class, 2l );
					if ( !isOrientDBEx ) {
						// delete thread get exception and not delete entity
						assertNull( "Writer must be deleted!", writer );
					}
					em.getTransaction().commit();
				}
				catch (Exception e) {
					log.error( "Error", e );
					em.getTransaction().rollback();
					throw e;
				}
			}
			else {
				assertTrue( "No success threads!", false );
			}
		}
	}

	private static boolean isNotActualVersion(RollbackException re) {

		boolean isNotActualVersion = false;
		Throwable t = searchThrowable( re, HibernateException.class );
		log.debug( "!HibernateException", t );
		if ( t != null ) {
			HibernateException he = (HibernateException) t;
			isNotActualVersion = ( he.getMessage().contains( "OGM001716" ) );
		}
		return isNotActualVersion;
	}

	private static boolean isOptimisticLockException(RollbackException re) {
		// return re.getCause().getCause() instanceof OptimisticLockException;
		Throwable t = searchThrowable( re, OptimisticLockException.class );
		log.debug( "!OptimisticLockException", t );
		return t != null;
	}

	private static boolean isOConcurrentModificationException(RollbackException re) {
		// return re.getCause().getCause() instanceof OConcurrentModificationException || re.getCause() instanceof
		// OConcurrentModificationException;
		Throwable t = searchThrowable( re, OConcurrentModificationException.class );
		log.debug( "!OConcurrentModificationException", t );
		return t != null;
	}

	private static Throwable searchThrowable(Throwable currentThrowable, Class requiredThrowableClass) {
		Throwable requiredThrowable = null;
		if ( currentThrowable.getClass().equals( requiredThrowableClass ) ) {
			requiredThrowable = currentThrowable;
		}
		else if ( currentThrowable.getCause() != null ) {
			requiredThrowable = searchThrowable( currentThrowable.getCause(), requiredThrowableClass );
		}

		return requiredThrowable;
	}

	private boolean isAnyThreadSuccess(ForkJoinTask<Long> t1, ForkJoinTask<Long> t2) {
		return t1.isCompletedNormally() || t2.isCompletedNormally();
	}

	private class WriterUpdateThread implements Callable<Long> {

		private final Logger log = Logger.getLogger( WriterUpdateThread.class.getName() );
		private final long taskId;
		private final long writerId;
		private final EntityManager localEm;
		private final Set<Long> readyForWrite;

		public WriterUpdateThread(long taskId, long writerId, EntityManager localEm, Set<Long> readyForWrite) {
			this.taskId = taskId;
			this.writerId = writerId;
			this.localEm = localEm;
			this.readyForWrite = readyForWrite;
		}

		@Override
		public Long call() throws Exception {
			try {
				log.info( "begin reading..." );
				localEm.getTransaction().begin();
				Query query = localEm.createNativeQuery( "select from writer where bKey=" + writerId, Writer.class );
				List<Writer> results = query.getResultList();
				assertFalse( "Writer must be!", results.isEmpty() );
				readyForWrite.add( taskId );
				log.info( "Waiting other threads..." );
				while ( readyForWrite.size() != 2 ) {
					try {
						Thread.sleep( 100 );
					}
					catch (InterruptedException ie) {
					}
				}
				log.info( "All threads ready. Lets go!" );

				Writer writer = results.get( 0 );
				writer.setCount( taskId );
				writer = localEm.merge( writer );

				log.info( "begin writing...." );
				localEm.getTransaction().commit();
				log.info( "transaction commited" );
			}
			catch (Exception e) {
				if ( localEm.getTransaction().isActive() ) {
					log.info( "try to rollback transaction" );
					localEm.getTransaction().rollback();
				}
				throw e;
			}
			finally {
				localEm.clear();
				localEm.close();
			}
			return taskId;
		}
	}

	private class WriterDeleteThread implements Callable<Long> {

		private final Logger log = Logger.getLogger( WriterDeleteThread.class.getName() );
		private final long taskId;
		private final long writerId;
		private final EntityManager localEm;
		private final Set<Long> readyForWrite;

		public WriterDeleteThread(long taskId, long writerId, EntityManager localEm, Set<Long> readyForWrite) {
			this.taskId = taskId;
			this.writerId = writerId;
			this.localEm = localEm;
			this.readyForWrite = readyForWrite;
		}

		@Override
		public Long call() throws Exception {
			try {
				log.info( "begin reading..." );
				localEm.getTransaction().begin();
				Query query = localEm.createNativeQuery( "select from writer where bKey=" + writerId, Writer.class );
				List<Writer> results = query.getResultList();
				assertFalse( "Writer must be!", results.isEmpty() );
				readyForWrite.add( taskId );
				log.info( "Waiting other threads..." );
				while ( readyForWrite.size() != 2 ) {
					try {
						Thread.sleep( 100 );
					}
					catch (InterruptedException ie) {
					}
				}
				log.info( "All threads ready. Lets go!" );
				Writer writer = results.get( 0 );
				localEm.remove( writer );
				log.info( "begin writing...." );
				localEm.getTransaction().commit();
				log.info( "transaction commited" );
			}
			catch (Exception e) {
				if ( localEm.getTransaction().isActive() ) {
					log.info( "try to rollback transaction" );
					localEm.getTransaction().rollback();
				}
				throw e;
			}
			finally {
				localEm.clear();
				localEm.close();
			}
			return taskId;
		}
	}

}