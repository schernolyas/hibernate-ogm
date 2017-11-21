/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
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
import org.hibernate.type.ManyToOneType;
import org.hibernate.type.Type;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
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
					CacheConfiguration config = createEntityCacheConfiguration( entityKeyMetadata, context );
					igniteDatastoreProvider.initializeCache( config );
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
		List<CacheKeyConfiguration> cacheKeyConfigurations = new LinkedList<>(  );
		appendIndex( queryEntity, associationKeyMetadata, context, cacheKeyConfigurations );


		log.infof( "createEntityCacheConfiguration. full QueryEntity info :%s;", queryEntity.toString() );
		result.setQueryEntities( Arrays.asList( queryEntity ) );

		if ( !cacheKeyConfigurations.isEmpty() ) {
			result.setKeyConfiguration( cacheKeyConfigurations.toArray( new CacheKeyConfiguration[cacheKeyConfigurations.size()] ) );
		}

		return result;
	}

	private List<String> appendIndex(QueryEntity queryEntity, AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context,
			List<CacheKeyConfiguration> cacheKeyConfigurations) {
		List<String> affinityFields = Collections.emptyList();
		SessionFactoryImplementor sessionFactory = context.getSessionFactory();
		List<QueryIndex> queryIndices = new ArrayList<>( associationKeyMetadata.getRowKeyColumnNames().length );
		log.debugf( "associationKeyMetadata: %s ", associationKeyMetadata );
		for ( String idFieldName : associationKeyMetadata.getColumnNames() ) {
			log.debugf( "idFieldName: %s , %s", idFieldName, generateIndexName( idFieldName ) );
			log.debugf( "fields : %s", queryEntity.getFields().keySet() );
			Type associationKeyType = findAssociationKeyType( context, associationKeyMetadata.getTable(), idFieldName );

			if ( associationKeyType instanceof ManyToOneType ) {
				affinityFields = new LinkedList<>(  );
				ManyToOneType type = (ManyToOneType) associationKeyType;
				Type t1 = type.getSemiResolvedType( sessionFactory );
				queryEntity.addQueryField( generateIndexName( idFieldName ), t1.getReturnedClass().getName(), null );
				log.debugf( "add field  %s and index for them", idFieldName );
				queryIndices.add( new QueryIndex( generateIndexName( idFieldName ), QueryIndexType.SORTED ) );
				affinityFields.add( generateIndexName( idFieldName ) );

				CacheKeyConfiguration cacheKeyConfiguration = new CacheKeyConfiguration(  );
				cacheKeyConfiguration.setTypeName( queryEntity.getValueType() ).setAffinityKeyFieldName( generateIndexName( idFieldName ) );
				cacheKeyConfigurations.add( cacheKeyConfiguration );
			}
			else {
				log.debugf( "add index for field %s", idFieldName );
				queryIndices.add( new QueryIndex( generateIndexName( idFieldName ), QueryIndexType.SORTED ) );
			}

		}
		queryEntity.setIndexes( queryIndices );
		return affinityFields;
	}

	private Type findAssociationKeyType(SchemaDefinitionContext context, String table, String fieldName) {
		for ( Iterator<Namespace> nsIt = context.getDatabase().getNamespaces().iterator(); nsIt.hasNext(); ) {
			Namespace currentNamespace = nsIt.next();
			log.debugf( "n. Namespace : %s", currentNamespace.toString() );
			for ( Table currentTable : currentNamespace.getTables() ) {
				log.debugf( "table : %s", currentTable.getName() );
				if ( currentTable.getName().equals( table ) ) {
					for ( Iterator<Column> it = currentTable.getColumnIterator(); it.hasNext(); ) {
						Column column = it.next();
						if ( column.getName().equals( fieldName ) ) {
							return column.getValue().getType();

						}

					}

				}

			}
		}
		return null;
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

	private CacheConfiguration<?,?> createEntityCacheConfiguration(EntityKeyMetadata entityKeyMetadata, SchemaDefinitionContext context) {
		log.debugf( "entityKeyMetadata: %s", entityKeyMetadata );
		OptionsService optionsService = context.getSessionFactory().getServiceRegistry().getService( OptionsService.class );
		//@todo refactor it!
		Class<?> entityType = tableEntityTypeMapping.get( entityKeyMetadata.getTable() );
		log.debugf( "initialize cache for entity class %s",entityType.getName() );



		CacheConfiguration<?,?> cacheConfiguration = new CacheConfiguration<>();
		cacheConfiguration.setStoreKeepBinary( true );
		List<CacheKeyConfiguration> cacheKeyConfigurations = new LinkedList<>(  );


		cacheConfiguration.setName( StringHelper.stringBeforePoint( entityKeyMetadata.getTable() ) );
		cacheConfiguration.setAtomicityMode( CacheAtomicityMode.TRANSACTIONAL );
		cacheConfiguration.setSqlSchema( QueryUtils.DFLT_SCHEMA );//@todo not forget about schemas for entity
		cacheConfiguration.setBackups( 1 );


		QueryEntity queryEntity = new QueryEntity( getEntityIdClassName( entityKeyMetadata.getTable(), context ), entityType );
		log.debugf( "createEntityCacheConfiguration. create QueryEntity for table:%s;", entityKeyMetadata.getTable() );

		addTableInfo( queryEntity, context, entityKeyMetadata.getTable() );

		for ( AssociationKeyMetadata associationKeyMetadata : context.getAllAssociationKeyMetadata() ) {
			if ( associationKeyMetadata.getAssociationKind() != AssociationKind.EMBEDDED_COLLECTION
					&& associationKeyMetadata.getTable().equals( entityKeyMetadata.getTable() )
					&& !IgniteAssociationSnapshot.isThirdTableAssociation( associationKeyMetadata ) ) {
				appendIndex( queryEntity, associationKeyMetadata, context, cacheKeyConfigurations );

			}
		}
		log.infof( "createEntityCacheConfiguration. full QueryEntity info :%s;", queryEntity.toString() );
		cacheConfiguration.setQueryEntities( Arrays.asList( queryEntity ) );
		if ( !cacheKeyConfigurations.isEmpty() ) {
			cacheConfiguration.setKeyConfiguration( cacheKeyConfigurations.toArray( new CacheKeyConfiguration[cacheKeyConfigurations.size()] ) );
		}

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
