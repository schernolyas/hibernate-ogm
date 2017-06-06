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
import java.util.List;
import java.util.Map;

import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.hibernate.HibernateException;
import org.hibernate.ogm.datastore.ignite.IgniteProperties;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.ignite.options.Searchable;
import org.hibernate.ogm.datastore.ignite.options.impl.CacheStoreFactoryOption;
import org.hibernate.ogm.datastore.ignite.options.impl.ReadThroughOption;
import org.hibernate.ogm.datastore.ignite.options.impl.StoreKeepBinaryOption;
import org.hibernate.ogm.datastore.ignite.options.impl.WriteThroughOption;
import org.hibernate.ogm.datastore.ignite.util.ClassUtil;
import org.hibernate.ogm.datastore.ignite.util.StringHelper;
import org.hibernate.ogm.datastore.spi.BaseSchemaDefiner;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata.IdSourceType;
import org.hibernate.ogm.options.spi.OptionsService;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.persister.entity.EntityPersister;

/**
 * @author Victor Kadachigov
 */
public class IgniteCacheInitializer extends BaseSchemaDefiner {

	private static final long serialVersionUID = -8564869898957031491L;
	private static final Log log = LoggerFactory.getLogger();
	private static Map<String, Class<?>> tableEntityTypeMapping;

	public static Map<String, Class<?>> getTableEntityTypeMapping() {
		return Collections.unmodifiableMap( tableEntityTypeMapping );
	}

	@Override
	public void initializeSchema(SchemaDefinitionContext context) {
		tableEntityTypeMapping = context.getTableEntityTypeMapping();

		DatastoreProvider provider = context.getSessionFactory().getServiceRegistry().getService( DatastoreProvider.class );
		if ( provider instanceof IgniteDatastoreProvider ) {
			IgniteDatastoreProvider igniteDatastoreProvider = (IgniteDatastoreProvider) provider;
			initializeEntities( context, igniteDatastoreProvider );
			initializeAssociations( context, igniteDatastoreProvider );
			initializeIdSources( context, igniteDatastoreProvider );
		}
		else {
			log.unexpectedDatastoreProvider( provider.getClass(), IgniteDatastoreProvider.class );
		}
	}

	private void initializeEntities(SchemaDefinitionContext context, final IgniteDatastoreProvider igniteDatastoreProvider) {
		ConfigurationPropertyReader propertyReader = igniteDatastoreProvider.getPropertyReader();


		for ( EntityKeyMetadata entityKeyMetadata : context.getAllEntityKeyMetadata() ) {

			try {
				try {

					igniteDatastoreProvider.getEntityCache( entityKeyMetadata );
				}
				catch (HibernateException ex) {

					CacheConfiguration config = createEntityCacheConfiguration( entityKeyMetadata, context, propertyReader );
					igniteDatastoreProvider.initializeCache( config );
					createReadThroughIndexConfiguration( entityKeyMetadata, context ).forEach( cacheConfiguration -> {
						igniteDatastoreProvider.initializeCache( cacheConfiguration );
					} );
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

	private Boolean getReadThroughOptionValue(OptionsService optionsService, Class<?> entityType) {
		return optionsService.context().getEntityOptions( entityType ).getUnique( ReadThroughOption.class );
	}

	private Boolean getWriteThroughOptionValue(OptionsService optionsService, Class<?> entityType) {
		return optionsService.context().getEntityOptions( entityType ).getUnique( WriteThroughOption.class );
	}

	private Boolean getStoreKeepBinaryOptionValue(OptionsService optionsService, Class<?> entityType) {
		return optionsService.context().getEntityOptions( entityType ).getUnique( StoreKeepBinaryOption.class );
	}

	private Class getCacheStoreFactoryOptionValue(OptionsService optionsService, Class<?> entityType) {
		return optionsService.context().getEntityOptions( entityType ).getUnique( CacheStoreFactoryOption.class );
	}

	private void initializeIdSources(SchemaDefinitionContext context, IgniteDatastoreProvider igniteDatastoreProvider) {
		ConfigurationPropertyReader propertyReader = igniteDatastoreProvider.getPropertyReader();
		for ( IdSourceKeyMetadata idSourceKeyMetadata : context.getAllIdSourceKeyMetadata() ) {
			if ( idSourceKeyMetadata.getType() == IdSourceType.TABLE ) {
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
		}
	}

	private CacheConfiguration createCacheConfiguration(IdSourceKeyMetadata idSourceKeyMetadata) {
		CacheConfiguration result = new CacheConfiguration();
		result.setName( StringHelper.stringBeforePoint( idSourceKeyMetadata.getName() ) );
		return result;
	}

	private CacheConfiguration createCacheConfiguration(AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context,
														ConfigurationPropertyReader propertyReader) {

		if ( associationKeyMetadata.getColumnNames().length > 1 ) {
			//composite id. not yet implemented
			return null;
		}

		CacheConfiguration result = new CacheConfiguration();
		result.setName( StringHelper.stringBeforePoint( associationKeyMetadata.getTable() ) );
		//@todo configure cache for association table!

		QueryEntity queryEntity = new QueryEntity();
		queryEntity.setValueType( StringHelper.stringAfterPoint( associationKeyMetadata.getTable() ) );
		appendIndex( queryEntity, associationKeyMetadata, context );

		result.setQueryEntities( Arrays.asList( queryEntity ) );

		return result;
	}

	private void appendIndex(QueryEntity queryEntity, AssociationKeyMetadata associationKeyMetadata, SchemaDefinitionContext context) {

		if ( associationKeyMetadata.getColumnNames().length > 1 ) {
			//composite id. not yet implemented
			return;
		}

		String idFieldName = associationKeyMetadata.getColumnNames()[0];
		String idClassName = getEntityIdClassName( associationKeyMetadata.getEntityKeyMetadata().getTable(), context );
		queryEntity.addQueryField( idFieldName, idClassName, null );
		queryEntity.setIndexes( Arrays.asList( new QueryIndex( idFieldName, QueryIndexType.SORTED ) ) );
	}

	private String getEntityIdClassName( String table, SchemaDefinitionContext context ) {
		Class<?> entityClass = context.getTableEntityTypeMapping().get( table );
		EntityPersister entityPersister = context.getSessionFactory().getEntityPersister( entityClass.getName() );
		Class<?> idClass = entityPersister.getIdentifierType().getReturnedClass();
		return idClass.getName();
	}

	private CacheConfiguration createEntityCacheConfiguration(EntityKeyMetadata entityKeyMetadata, SchemaDefinitionContext context, ConfigurationPropertyReader propertyReader) {
		log.debugf( "entityKeyMetadata: %s", entityKeyMetadata );
		OptionsService optionsService = context.getSessionFactory().getServiceRegistry().getService( OptionsService.class );
		//@todo refactor it!
		Class<?> entityType = tableEntityTypeMapping.get( entityKeyMetadata.getTable() );
		log.debugf( "initialize cache for entity class %s",entityType.getName() );

		Boolean readThroughValue = getReadThroughOptionValue( optionsService, entityType );
		Boolean writeThroughValue = getWriteThroughOptionValue( optionsService, entityType );
		Boolean storeKeepBinaryValue = getStoreKeepBinaryOptionValue( optionsService, entityType );
		Class cacheStoreFactoryValue = getCacheStoreFactoryOptionValue( optionsService, entityType );
		log.debugf( "readThroughValue:%b;writeThroughValue:%b;",
					readThroughValue,writeThroughValue );

		CacheConfiguration cacheConfiguration = new CacheConfiguration();
		cacheConfiguration.setStoreKeepBinary( storeKeepBinaryValue );
		setCacheStoreFactory( cacheConfiguration, cacheStoreFactoryValue, entityType.getName(), propertyReader,readThroughValue, writeThroughValue );

		cacheConfiguration.setName( StringHelper.stringBeforePoint( entityKeyMetadata.getTable() ) );
		cacheConfiguration.setAtomicityMode( CacheAtomicityMode.TRANSACTIONAL );
		if ( !( readThroughValue || writeThroughValue ) ) {
			QueryEntity queryEntity = new QueryEntity();
			queryEntity.setKeyType( getEntityIdClassName( entityKeyMetadata.getTable(), context ) );
			queryEntity.setValueType( StringHelper.stringAfterPoint( entityKeyMetadata.getTable() ) );
			for ( AssociationKeyMetadata associationKeyMetadata : context.getAllAssociationKeyMetadata() ) {
				if ( associationKeyMetadata.getAssociationKind() != AssociationKind.EMBEDDED_COLLECTION
						&& associationKeyMetadata.getTable().equals( entityKeyMetadata.getTable() )
						&& !IgniteAssociationSnapshot.isThirdTableAssociation( associationKeyMetadata ) ) {
					appendIndex( queryEntity, associationKeyMetadata, context );
				}
			}
			cacheConfiguration.setQueryEntities( Arrays.asList( queryEntity ) );
		}
		return cacheConfiguration;
	}

	@SuppressWarnings("unchecked")
	private void setCacheStoreFactory( CacheConfiguration cacheConfiguration, Class cacheStoreFactoryValue, String entityName,
			ConfigurationPropertyReader propertyReader,Boolean readThroughValue,Boolean writeThroughValue) {
		try {
			log.infof( "set CacheStoreFactory  for Entity %s",  entityName );
			String factoryName = propertyReader.property( String.format( IgniteProperties.IGNITE_CACHE_STORE_FACTORY_TEMPLATE,
					entityName ), String.class ).getValue();
			log.infof( "1. factoryName: %s",  factoryName );
			if ( factoryName != null ) {
				// set factory class from properties file
				log.infof( "set CacheStoreFactory class %s for Entity %s", factoryName, entityName );
				Class<?> factoryClass = Class.forName( factoryName );
				cacheConfiguration.setCacheStoreFactory( (Factory<? extends CacheStore>) factoryClass.newInstance() );
				cacheConfiguration.setReadThrough( readThroughValue );
				cacheConfiguration.setWriteThrough( writeThroughValue );
			}
			else {
				String adapterClassName = propertyReader.property( String.format(
						IgniteProperties.IGNITE_CACHE_STORE_CLASS_TEMPLATE, entityName ), String.class )
						.getValue();
				log.infof( "2. adapterClassName: %s",  adapterClassName );
				if ( adapterClassName != null ) {
					// set adapter class from properties file
					log.infof( "set CacheStore class %s for Entity %s", adapterClassName, entityName );
					cacheConfiguration.setCacheStoreFactory( FactoryBuilder.factoryOf( Class.forName( adapterClassName ) ) );
					cacheConfiguration.setReadThrough( readThroughValue );
					cacheConfiguration.setWriteThrough( writeThroughValue );
					return;

				}
				log.infof( "3. cacheStoreFactoryValue: %s",  cacheStoreFactoryValue );
				if ( cacheStoreFactoryValue != null ) {
					// set adapter class from annotation
					log.infof( "set CacheStoreFactory class %s from annotation for Entity %s", cacheStoreFactoryValue, entityName );
					cacheConfiguration.setCacheStoreFactory( FactoryBuilder.factoryOf( cacheStoreFactoryValue ) );
					cacheConfiguration.setReadThrough( readThroughValue );
					cacheConfiguration.setWriteThrough( writeThroughValue );
					return;

				}
			}
		}
		catch (Exception e) {
			throw log.unableToInitializeCache( entityName, e );
		}
	}



	@SuppressWarnings("rawtypes")
	private List<CacheConfiguration> createReadThroughIndexConfiguration(EntityKeyMetadata entityKeyMetadata, SchemaDefinitionContext context) {
		OptionsService optionsService = context.getSessionFactory().getServiceRegistry().getService( OptionsService.class );
		Map<String, Class<?>> tableEntityTypeMapping = context.getTableEntityTypeMapping();
		Class<?> entityType = tableEntityTypeMapping.get( entityKeyMetadata.getTable() );
		log.debugf( "initialize read-through cache for index class %s", entityType.getName() );

		Boolean readThroughValue = getReadThroughOptionValue( optionsService, entityType );
		if ( !readThroughValue ) {
			return Collections.emptyList();
		}
		//it is read-through entity. search fields with annotation 'searchable'
		Field[] searchableFields = ClassUtil.getAnnotatedDeclaredFields( entityType, Searchable.class, false );
		List<CacheConfiguration> cacheConfigurations = new ArrayList<>( searchableFields.length );
		for ( int i = 0; i < searchableFields.length; i++ ) {
			log.debugf( "index: %d ; fieldName: %s", i, searchableFields[i].getName() );
			CacheConfiguration cacheConfiguration = new CacheConfiguration();
			//cacheConfiguration.setWriteThrough( true );
			//cacheConfiguration.setReadThrough( true );
			cacheConfiguration.setStoreKeepBinary( true );
			//cacheConfiguration.setCacheStoreFactory( FactoryBuilder.factoryOf( cacheStoreFactoryValue ) );

			cacheConfiguration.setName( generateIndexName(
					entityType.getSimpleName(),
					searchableFields[i].getName()
			) );
			cacheConfiguration.setAtomicityMode( CacheAtomicityMode.TRANSACTIONAL );
			cacheConfigurations.add( cacheConfiguration );
		}
		return cacheConfigurations;
	}

	public static String generateIndexName(String entityName, String fieldName) {
		return entityName + "_" + fieldName + "_inx";
	}
}
