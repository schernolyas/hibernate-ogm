/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.test.gridfs;

import java.sql.Blob;
import javax.persistence.EntityManager;

import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.ogm.datastore.mongodb.MongoDBDialect;
import org.hibernate.ogm.dialect.impl.OgmDialect;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.hibernatecore.impl.OgmSessionFactoryImpl;
import org.hibernate.ogm.utils.TestForIssue;
import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;

import org.junit.Test;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 * @see <a href="http://mongodb.github.io/mongo-java-driver/3.4/driver/tutorials/gridfs/">GridFSBucket</a>
 */
@TestForIssue(jiraKey = "OGM-786")
public class GridFsTest extends OgmJpaTestCase {

	@Test
	public void testSaveBlobToGridfs() {
		EntityManager em = getFactory().createEntityManager();
		try {
			em.getTransaction().begin();

			Photo photo = new Photo();
			photo.setId( "photo1" );
			photo.setDescription( "photo1" );
			Blob blob = Hibernate.getLobCreator( em.unwrap( Session.class ) ).createBlob( new byte[]{0,1,2,3,4,5,6,7,8,9}  );
			photo.setContent( blob );
			em.persist( photo );
			em.getTransaction().commit();
			MongoDatabase mongoDatabase = getCurrentDB(em);

			GridFSBucket gridFSFilesBucket = GridFSBuckets.create( mongoDatabase, "photos" );

			MongoCursor<GridFSFile>  cursor = gridFSFilesBucket.find().iterator();
			assertThat( cursor.hasNext() ).isEqualTo( true );

		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			em.close();
		}
	}

	private MongoDatabase getCurrentDB(EntityManager em) {
		Session session = em.unwrap(Session.class);
		OgmSessionFactoryImpl sessionFactory = (OgmSessionFactoryImpl) session.getSessionFactory();
		OgmDialect dialect = (OgmDialect) sessionFactory.getDialect();
		GridDialect gridDialect = dialect.getGridDialect();

		MongoDBDialect mongoDBDialect = (MongoDBDialect) gridDialect;
		return mongoDBDialect.getCurrentDB();
	}


	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[]{ Photo.class };
	}

}
