/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.impl;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.DependantValue;
import org.hibernate.mapping.Selectable;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.Value;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.ignite.util.ClassUtil;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;
import org.hibernate.ogm.datastore.spi.BaseSchemaDefiner;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata;
import org.hibernate.ogm.options.spi.OptionsService;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.persister.entity.EntityPersister;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;

/**
 * @author Victor Kadachigov
 */
public class IgniteCacheInitializer extends BaseSchemaDefiner {

	private static final long serialVersionUID = -8564869898957031491L;
	private static final Log log = LoggerFactory.getLogger();
	private static Map<String, Class<?>> tableEntityTypeMapping;
	private static Set<EntityKeyMetadata> entityKeyMetadata;
	private static Map<Class<?>, Class<?>> h2TypeMapping;

	static {
		Map<Class<?>, Class<?>> map = new HashMap<>(  );
		map.put( Character.class, String.class );

		h2TypeMapping = Collections.unmodifiableMap( map );
	}

	public static Map<String, Class<?>> getTableEntityTypeMapping() {
		return tableEntityTypeMapping;
	}

	public static Set<EntityKeyMetadata> getEntityKeyMetadata() {
		return entityKeyMetadata;
	}

	@Override
	public void initializeSchema(SchemaDefinitionContext context) {
		tableEntityTypeMapping = context.getTableEntityTypeMapping();
		log.debugf( "context.getAllEntityKeyMetadata(): %s", context.getAllEntityKeyMetadata() );
		log.debugf( "context.getAllAssociationKeyMetadata(): %s", context.getAllAssociationKeyMetadata() );
		log.debugf( "context.getAllIdSourceKeyMetadata(): %s", context.getAllIdSourceKeyMetadata() );
		log.debugf( "context.getTableEntityTypeMapping(): %s", context.getTableEntityTypeMapping() );

		DatastoreProvider provider = context.getSessionFactory().getServiceRegistry().getService( DatastoreProvider.class );
		if ( provider instanceof IgniteDatastoreProvider ) {
			IgniteDatastoreProvider igniteDatastoreProvider = (IgniteDatastoreProvider) provider;
			initializeEntities( context, igniteDatastoreProvider );
			initializeAssociations( context, igniteDatastoreProvider );
			initializeIdSources( context, igniteDatastoreProvider );
			entityKeyMetadata = context.getAllEntityKeyMetadata();
		}
		else {
			log.unexpectedDatastoreProvider( provider.getClass(), IgniteDatastoreProvider.class );
		}
	}

	private void initializeEntities(SchemaDefinitionContext context, final IgniteDatastoreProvider igniteDatastoreProvider) {
		ConfigurationPropertyReader propertyReader = igniteDatastoreProvider.getPropertyReader();


		for ( EntityKeyMetadata entityKeyMetadata : context.getAllEntityKeyMetadata() ) {
			log.debugf( "initializeEntities. current entityKeyMetadata: %s", entityKeyMetadata );

			try {
				try {
					log.debugf( "initializeEntities. try to get cache for entity: %s", entityKeyMetadata.getTable() );

					igniteDatastoreProvider.getEntityCache( entityKeyMetadata );
				}
				catch (HibernateException ex) {
					log.debugf( "initializeEntities. create schema for entity: %s", entityKeyMetadata.getTable() );
					CacheConfiguration config = createEntityCacheConfiguration( entityKeyMetadata, context, propertyReader );
					igniteDatastoreProvider.initializeCache( config );
					/*createReadThroughIndexConfiguration( entityKeyMetadata, context ).forEach( cacheConfiguration -> {
						igniteDatastoreProvider.initializeCache( cacheConfiguration );
					} ); */
				}
			}
			catch (Exception ex) {
				// just write error to log
				log.unableToInitializeCache( entityKeyMetadata.getTable(), ex );
			}
		}
	}

	private void initializeAssociations(SchemaDefinitionContext context, IgniteDatastoreProvider igniteDatastoreProvider) {

		ConfigurationPropertyReader propertyReader = igniteDatastoreProvider.getPropertyReader();
		for ( AssociationKeyMetadata associationKeyMetadata : context.getAllAssociationKeyMetadata() ) {
			log.debugf( "initializeAssociations. associationKeyMetadata: %s ",associationKeyMetadata );
			if ( associationKeyMetadata.getAssociationKind() != AssociationKind.EMBEDDED_COLLECTION
					&& IgniteAssociationSnapshot.isThirdTableAssociation( associationKeyMetadata ) ) {
				try {

					try {
						igniteDatastoreProvider.getAssociationCache( associationKeyMetadata );
					}
					catch (HibernateException ex) {
						CacheConfiguration config = createCacheConfiguration( associationKeyMetadata, context, propertyReader );
						if ( config != null ) {
							igniteDatastoreProvider.initializeCache( config );
						}
					}
				}
				catch (Exception ex) {
					// just write error to log
					log.unableToInitializeCache( associationKeyMetadata.getTable(), ex );
				}

			}
		}
	}



	private void initializeIdSources(SchemaDefinitionContext context, IgniteDatastoreProvider igniteDatastoreProvider) {
		ConfigurationPropertyReader propertyReader = igniteDatastoreProvider.getPropertyReader();
		//generate tables
		for ( IdSourceKeyMetadata idSourceKeyMetadata : context.getAllIdSourceKeyMetadata() ) {
			if ( idSourceKeyMetadata.getType() == IdSourceKeyMetadata.IdSourceType.TABLE ) {
				try {
					try {
						igniteDatastoreProvider.getIdSourceCache( idSourceKeyMetadata );
					}
					catch (HibernateException ex) {
						CacheConfiguration config = createCacheConfiguration( idSourceKeyMetadata );
						igniteDatastoreProvider.initializeCache( config );
					}
				}
				catch (Exception ex) {
					// just write error to log
					throw log.unableToInitializeCache( idSourceKeyMetadata.getName(), ex );
				}
			}
			else if ( idSourceKeyMetadata.getType() == IdSourceKeyMetadata.IdSourceType.SEQUENCE ) {
				log.debugf( "initializeIdSources. generate sequence: %s ",idSourceKeyMetadata.getName() );
				if ( idSourceKeyMetadata.getName() != null ) {
					igniteDatastoreProvider.atomicSequence( idSourceKeyMetadata.getName(),  1, true );
				}
			}
		}
		//generate sequences
		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			for ( Sequence sequence : namespace.getSequences() ) {
				log.debugf( "initializeIdSources. generate sequence: %s ",sequence.getName().getSequenceName().getCanonicalName() );
				igniteDatastoreProvider.atomicSequence( sequence.getName().getSequenceName().getCanonicalName(),  sequence.getInitialValue(), true );
			}
		}
	}

	private CacheConfiguration createCacheConfiguration(IdSourceKeyMetadata idSourceKeyMetadata) {
		CacheConfiguration result = new CacheConfiguration();
		result.setName( StringHelper.stringBeforePoint( idSourceKeyMetadata.getName() ) );
		return result;
	}

	private CacheConfiguration createCacheConfiguration(AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context,
			ConfigurationPropertyReader propertyReader) {

		CacheConfiguration result = new CacheConfiguration();
		result.setName( StringHelper.stringBeforePoint( associationKeyMetadata.getTable() ) );

		QueryEntity queryEntity = new QueryEntity();
		queryEntity.setTableName( associationKeyMetadata.getTable() );
		queryEntity.setValueType( StringHelper.stringAfterPoint( associationKeyMetadata.getTable() ) );
		appendIndex( queryEntity, associationKeyMetadata, context );

		result.setQueryEntities( Arrays.asList( queryEntity ) );

		return result;
	}

	private void appendIndex(QueryEntity queryEntity, AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context) {

		for ( Iterator<Namespace> nsIt = context.getDatabase().getNamespaces().iterator(); nsIt.hasNext(); ) {
			Namespace namespace = nsIt.next();
			log.debugf( "appendIndex. Namespace : %s", namespace.toString() );
			for ( Table table : namespace.getTables() ) {
				log.debugf( "appendIndex. Table : %s", table.getName() );
				for ( Iterator<Column> it = table.getColumnIterator(); it.hasNext(); ) {
					Column column = it.next();
					log.debugf( "appendIndex. column name: %s; column value: %s", column.getName(), column.getValue() );
					if ( column.getValue().getClass() == SimpleValue.class ) {
						SimpleValue simpleValue = (SimpleValue) column.getValue();
						log.debugf( "appendIndex. column name: %s; column value type: %s", column.getName(), simpleValue.getType().getReturnedClass() );
					}
					else if ( column.getValue().getClass() == DependantValue.class ) {
						DependantValue dv = (DependantValue) column.getValue();
						for ( Iterator<Selectable> iterator = dv.getColumnIterator(); iterator.hasNext(); ) {
							Column column1 = (Column) iterator.next();
							log.debugf( "appendIndex. column name: %s; column value type: %s", column.getName(), column1.getValue().getType().getReturnedClass() );
						}
					}
				}
			}
		}

		Class entityIdClass = getEntityIdClassName( associationKeyMetadata.getEntityKeyMetadata().getTable(), context );
		for ( Field f : ClassUtil.getDeclaredFields( entityIdClass, true ) ) {
			log.debugf( "appendIndex. field: name: %s , type : %s", f.getName(),f.getType().getName() );
		}

		for ( String idFieldName : associationKeyMetadata.getRowKeyColumnNames() ) {
			log.debugf( "appendIndex. idFieldName: %s , %s",idFieldName, generateIndexName( idFieldName ) );
			queryEntity.addQueryField( generateIndexName( idFieldName ), String.class.getName(),null );
			queryEntity.setIndexes( Arrays.asList( new QueryIndex( generateIndexName( idFieldName ), QueryIndexType.SORTED  ) ) );
		}
	}

	private String generateIndexName(String fieldName) {
		return fieldName.replace( '.','_' );
	}

	private Class getEntityIdClassName( String table, SchemaDefinitionContext context ) {
		Class<?> entityClass = context.getTableEntityTypeMapping().get( table );
		for ( Field f : ClassUtil.getDeclaredFields( entityClass, false ) ) {
			log.debugf( "getEntityIdClassName.  entity field: %s ", f );
		}
		EntityPersister entityPersister = context.getSessionFactory().getEntityPersister( entityClass.getName() );
		return entityPersister.getIdentifierType().getReturnedClass();
	}

	private CacheConfiguration<?,?> createEntityCacheConfiguration(EntityKeyMetadata entityKeyMetadata, SchemaDefinitionContext context, ConfigurationPropertyReader propertyReader) {
		log.debugf( "entityKeyMetadata: %s", entityKeyMetadata );
		OptionsService optionsService = context.getSessionFactory().getServiceRegistry().getService( OptionsService.class );
		//@todo refactor it!
		Class<?> entityType = tableEntityTypeMapping.get( entityKeyMetadata.getTable() );
		log.debugf( "initialize cache for entity class %s",entityType.getName() );



		CacheConfiguration<?,?> cacheConfiguration = new CacheConfiguration<>();
		cacheConfiguration.setStoreKeepBinary( true );


		cacheConfiguration.setName( StringHelper.stringBeforePoint( entityKeyMetadata.getTable() ) );
		cacheConfiguration.setAtomicityMode( CacheAtomicityMode.TRANSACTIONAL );
		cacheConfiguration.setSqlSchema( QueryUtils.DFLT_SCHEMA );//@todo not forget about schemas for entity
		cacheConfiguration.setBackups( 1 );

			QueryEntity queryEntity = new QueryEntity( getEntityIdClassName( entityKeyMetadata.getTable(), context ), entityType );
			log.debugf( "createEntityCacheConfiguration. create QueryEntity for table:%s;",
					entityKeyMetadata.getTable() );
			//queryEntity.setTableName( entityKeyMetadata.getTable() );
			// queryEntity.setKeyType( getEntityIdClassName( entityKeyMetadata.getTable(), context ).getSimpleName() );
			// queryEntity.setValueType( StringHelper.stringAfterPoint( entityKeyMetadata.getTable() ) );
			addTableInfo( queryEntity, context, entityKeyMetadata.getTable() );

			for ( AssociationKeyMetadata associationKeyMetadata : context.getAllAssociationKeyMetadata() ) {
				if ( associationKeyMetadata.getAssociationKind() != AssociationKind.EMBEDDED_COLLECTION
						&& associationKeyMetadata.getTable().equals( entityKeyMetadata.getTable() )
						&& !IgniteAssociationSnapshot.isThirdTableAssociation( associationKeyMetadata ) ) {
					appendIndex( queryEntity, associationKeyMetadata, context );
				}
			}
			log.infof( "createEntityCacheConfiguration. full QueryEntity info :%s;",
					queryEntity.toString() );
			cacheConfiguration.setQueryEntities( Arrays.asList( queryEntity ) );

		return cacheConfiguration;
	}

	@SuppressWarnings("unchecked")
	private void addTableInfo(QueryEntity queryEntity, SchemaDefinitionContext context, String tableName) {
		Namespace namespace = context.getDatabase().getDefaultNamespace();
		Optional<Table> tableOptional = namespace.getTables().stream().filter( currentTable -> currentTable.getName().equals( tableName ) ).findFirst();
		if ( tableOptional.isPresent() ) {
			Table table = tableOptional.get();
			for ( Iterator<Column> columnIterator = table.getColumnIterator(); columnIterator.hasNext();) {
				Column currentColumn = columnIterator.next();
				Value value = currentColumn.getValue();
				if ( value.getClass() == SimpleValue.class ) {
					// it is simple type. add the field
					SimpleValue simpleValue = (SimpleValue) value;
					Class returnValue = simpleValue.getType().getReturnedClass();
					returnValue = h2TypeMapping.getOrDefault( returnValue , returnValue  );
					queryEntity.addQueryField( currentColumn.getName(),returnValue.getName(),null );
				}
			}
		}
	}


}
