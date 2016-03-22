/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.RowKey;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBAssociationQueries extends QueriesBase {

	private static final Log log = LoggerFactory.getLogger();

	private final EntityKeyMetadata ownerEntityKeyMetadata;
	private final AssociationKeyMetadata associationKeyMetadata;

	public OrientDBAssociationQueries(EntityKeyMetadata ownerEntityKeyMetadata, AssociationKeyMetadata associationKeyMetadata) {
		this.ownerEntityKeyMetadata = ownerEntityKeyMetadata;
		this.associationKeyMetadata = associationKeyMetadata;
	}

	public List<Map<String, Object>> findRelationship(Connection connection, AssociationKey associationKey, RowKey rowKey) {
		Map<String, Object> relationshipValues = new LinkedHashMap<>();
		if ( associationKey.getMetadata().getRowKeyIndexColumnNames().length > 0 ) {
			// int length = associationKey.getMetadata().getRowKeyIndexColumnNames().length;
			String[] indexColumnNames = associationKey.getMetadata().getRowKeyIndexColumnNames();
			for ( int i = 0; i < indexColumnNames.length; i++ ) {
				for ( int j = 0; j < rowKey.getColumnNames().length; j++ ) {
					if ( indexColumnNames[i].equals( rowKey.getColumnNames()[j] ) ) {
						relationshipValues.put( rowKey.getColumnNames()[j], rowKey.getColumnValues()[j] );
					}
				}
			}
		}
		else {
			EntityKey entityKey = getEntityKey( associationKey, rowKey );
			for ( int i = 0; i < entityKey.getColumnNames().length; i++ ) {
				relationshipValues.put( entityKey.getColumnNames()[i], entityKey.getColumnValues()[i] );
			}
		}
		for ( String columnName : associationKey.getColumnNames() ) {
			relationshipValues.put( columnName, associationKey.getColumnValue( columnName ) );
		}

		for ( Map.Entry<String, Object> entry : relationshipValues.entrySet() ) {
			String key = entry.getKey();
			Object value = entry.getValue();
			log.debugf( "findRelationship: key %s; value: %s", key, value );
		}
		List<Map<String, Object>> dbValues = new LinkedList<>();
		StringBuilder queryBuilder = new StringBuilder( 100 );
		queryBuilder.append( "SELECT  FROM " ).append( associationKey.getTable() );
		queryBuilder.append( " WHERE " );
		int index = 0;
		for ( Map.Entry<String, Object> entry : relationshipValues.entrySet() ) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if ( index > 0 ) {
				queryBuilder.append( " AND " );
			}
			queryBuilder.append( key ).append( "=" );
			EntityKeyUtil.setFieldValue( queryBuilder, value );
			index++;
		}
		log.debugf( "findRelationship: queryBuilder: %s", queryBuilder );
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery( queryBuilder.toString() );
			while ( rs.next() ) {
				ResultSetMetaData metadata = rs.getMetaData();
				Map<String, Object> rowValues = new LinkedHashMap<>();
				for ( String systemField : OrientDBConstant.SYSTEM_FIELDS ) {
					rowValues.put( systemField, rs.getObject( systemField ) );
				}
				for ( int i = 0; i < rs.getMetaData().getColumnCount(); i++ ) {
					int dbFieldNo = i + 1;
					String dbColumnName = metadata.getColumnName( dbFieldNo );
					rowValues.put( dbColumnName, rs.getObject( dbColumnName ) );
				}
				dbValues.add( rowValues );
			}
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( queryBuilder.toString(), sqle );

		}
		log.debugf( "findRelationship: found: %d", dbValues.size() );

		return dbValues;
	}

	private EntityKey getEntityKey(AssociationKey associationKey, RowKey rowKey) {
		String[] associationKeyColumns = associationKey.getMetadata().getAssociatedEntityKeyMetadata().getAssociationKeyColumns();
		Object[] columnValues = new Object[associationKeyColumns.length];
		int i = 0;

		for ( String associationKeyColumn : associationKeyColumns ) {
			columnValues[i] = rowKey.getColumnValue( associationKeyColumn );
			i++;
		}

		EntityKeyMetadata entityKeyMetadata = associationKey.getMetadata().getAssociatedEntityKeyMetadata().getEntityKeyMetadata();
		return new EntityKey( entityKeyMetadata, columnValues );
	}
}
