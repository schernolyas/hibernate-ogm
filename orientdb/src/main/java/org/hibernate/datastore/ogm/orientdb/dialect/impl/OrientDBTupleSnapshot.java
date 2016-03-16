/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.dto.EmbeddedColumnInfo;

import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.AssociationUtil;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.model.key.spi.AssociatedEntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.TupleSnapshot;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBTupleSnapshot implements TupleSnapshot {

	private static Log log = LoggerFactory.getLogger();
	private final Map<String, Object> dbNameValueMap;

	private Map<String, AssociatedEntityKeyMetadata> associatedEntityKeyMetadata;
	private Map<String, String> rolesByColumn;
	private EntityKeyMetadata entityKeyMetadata;

	public OrientDBTupleSnapshot(Map<String, Object> dbNameValueMap,
			Map<String, AssociatedEntityKeyMetadata> associatedEntityKeyMetadata,
			Map<String, String> rolesByColumn,
			EntityKeyMetadata entityKeyMetadata) {
		this.dbNameValueMap = dbNameValueMap;
		this.associatedEntityKeyMetadata = associatedEntityKeyMetadata;
		this.rolesByColumn = rolesByColumn;
		this.entityKeyMetadata = entityKeyMetadata;
		log.debugf( "1.dbNameValueMap: %s" ,dbNameValueMap );
		log.debugf( "1.associatedEntityKeyMetadata: %s" ,associatedEntityKeyMetadata );
		log.debugf( "1.rolesByColumn: %s" , rolesByColumn );
	}

	public OrientDBTupleSnapshot(Map<String, AssociatedEntityKeyMetadata> associatedEntityKeyMetadata,
			Map<String, String> rolesByColumn,
			EntityKeyMetadata entityKeyMetadata) {
		this( new HashMap<String, Object>(), associatedEntityKeyMetadata, rolesByColumn, entityKeyMetadata );
		log.debugf( "2.dbNameValueMap: %s" , dbNameValueMap );
		log.debugf( "2.associatedEntityKeyMetadata: %s" , associatedEntityKeyMetadata );
		log.debugf( "2.rolesByColumn: %s" , rolesByColumn );
	}

	@Override
	public Object get(String targetColumnName) {
		log.debugf( "targetColumnName: %s" ,targetColumnName );
		Object value = null;
		if ( targetColumnName.equals( OrientDBConstant.SYSTEM_VERSION ) && value == null ) {
			value = Integer.valueOf( 0 );
		} else if (EntityKeyUtil.isEmbeddedColumn( targetColumnName )) {
                    //@TODO think about optimization
                    EmbeddedColumnInfo ec = new EmbeddedColumnInfo(targetColumnName);
                    log.debugf( "embedded column. class: %s ; property: %s", ec.getClassName(), ec.getPropertyName() );
                    JSONParser parser = new JSONParser();
                     try {
                        JSONObject jsonObj = (JSONObject) parser.parse((String) dbNameValueMap.get( ec.getClassName() ));
                        value = jsonObj.get(ec.getPropertyName());
                        log.debugf( "targetColumnName: %s ; value: %s; class : %s", targetColumnName,value,( value != null ? value.getClass() : null ) );
                    } catch (ParseException pe) {
                        throw log.cannotParseEmbeddedClass(targetColumnName,(String) dbNameValueMap.get( ec.getClassName() ), pe);
                    }
                    
                }
		else {
			value = dbNameValueMap.get( targetColumnName );
			log.debugf( "targetColumnName: %s ; value: %s; class : ", targetColumnName,value,( value != null ? value.getClass() : null ) );
		}
		return value;
	}

	private Map<String, Object> loadAssociatedEntity(AssociatedEntityKeyMetadata associatedEntityKeyMetadata, String targetColumnName) {
		String mappedByName = AssociationUtil.getMappedByFieldName( associatedEntityKeyMetadata );
		String inOrientDbField = "in_".concat( mappedByName );
		log.debug( "mappedByName: " + mappedByName + "; inOrientDbField:" + inOrientDbField );
		log.debug( "inOrientDbField: " + inOrientDbField + ".loaded? :" + dbNameValueMap.containsKey( inOrientDbField ) );
		Map<String, Object> value = null;
		if ( dbNameValueMap.containsKey( inOrientDbField ) ) {
			value = (Map<String, Object>) dbNameValueMap.get( inOrientDbField );
			log.debug( "value: " + value.getClass() );
		}
		return value;
	}

	@Override
	public boolean isEmpty() {
		return dbNameValueMap.isEmpty();
	}

	@Override
	public Set<String> getColumnNames() {
		return dbNameValueMap.keySet();
	}

	/**
	 * Whether this snapshot has been newly created (meaning it doesn't have an actual {@link Node} yet) or not. A node
	 * will be in the "new" state between the {@code createTuple()} call and the next {@code insertOrUpdateTuple()}
	 * call.
	 */
	public boolean isNew() {
		return dbNameValueMap == null || ( dbNameValueMap != null && dbNameValueMap.isEmpty() );
	}

}
