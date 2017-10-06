/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.parsing.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.AssertionFailure;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.persister.impl.OgmCollectionPersister;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;
import org.hibernate.ogm.query.parsing.impl.ParserPropertyHelper;
import org.hibernate.ogm.util.impl.ArrayHelper;
import org.hibernate.ogm.util.impl.StringHelper;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Joinable;
import org.hibernate.type.AssociationType;
import org.hibernate.type.EntityType;
import org.hibernate.type.Type;

/**
 * @author Victor Kadachigov
 */
public class IgnitePropertyHelper extends ParserPropertyHelper {

	private static final Log log = LoggerFactory.getLogger();
	private final Map<String, String> aliasByEntityName = new LinkedHashMap<>();
	private final Map<String, RelationshipAliasTree> relationshipAliases = new HashMap<>();
	private final SessionFactoryImplementor sessionFactory;
	private int relationshipCounter = 0;

	// Contains the aliases that will appear in the MATCH clause of the query
	private final Set<String> requiredMatches = new HashSet<>();

	public IgnitePropertyHelper(SessionFactoryImplementor sessionFactory, EntityNamesResolver entityNames ) {
		super( sessionFactory, entityNames );
		this.sessionFactory = sessionFactory;
		log.debugf( "====create new instance!====" );
	}

	@Override
	public Object convertToBackendType(String entityType, List<String> propertyPath, Object value) {
		log.infof( "entityType:%s; propertyPath:%s; value:%s ",entityType,propertyPath,value );
		return value == PropertyIdentifier.PARAM_INSTANCE
				? value : super.convertToBackendType( entityType, propertyPath, value );
	}

	/**
	 * Returns the {@link PropertyIdentifier} for the given property path.
	 *
	 * In passing, it creates all the necessary aliases for embedded/associations.
	 *
	 * @param entityType the type of the entity
	 * @param propertyPath the path to the property without aliases
	 * @param requiredDepth it defines until where the aliases will be considered as required aliases (see {@link IgniteAliasResolver} for more information)
	 * @return the {@link PropertyIdentifier}
	 */


	public PropertyIdentifier getPropertyIdentifier(String entityType, List<String> propertyPath, int requiredDepth) {
		log.infof( "entityType:%s ; propertyPath:%s ; requiredDepth:%s",
				entityType,propertyPath,requiredDepth );
		// we analyze the property path to find all the associations/embedded which are in the way and create proper
		// aliases for them
		String entityAlias = findAliasForType( entityType );

		String propertyEntityType = entityType;
		String propertyAlias = entityAlias;
		String propertyName;

		List<String> currentPropertyPath = new ArrayList<>();
		List<String> lastAssociationPath = Collections.emptyList();
		OgmEntityPersister currentPersister = getPersister( entityType );

		boolean isLastElementAssociation = false;
		int depth = 1;
		for ( String currentProperty : propertyPath ) {
			currentPropertyPath.add( currentProperty );
			Type currentPropertyType = getPropertyType( entityType, currentPropertyPath );
			log.infof( "currentProperty: %s", currentProperty );
			log.infof( "currentPropertyPath: %s", currentPropertyPath );
			log.infof( "currentPersister.getTableName(): %s", currentPersister.getTableName() );
			log.infof( "currentPersister.getDiscriminatorColumnName(): %s", currentPersister.getDiscriminatorColumnName() );
			log.infof( "currentPersister.getDiscriminatorType(): %s", currentPersister.getDiscriminatorType() );

			// determine if the current currentProperty path is still part of requiredPropertyMatch
			//boolean optionalMatch = depth > requiredDepth;

			if ( currentPropertyType.isAssociationType() ) {
				AssociationType associationPropertyType = (AssociationType) currentPropertyType;
				Joinable associatedJoinable = associationPropertyType.getAssociatedJoinable( getSessionFactory() );
				if ( associatedJoinable.isCollection()
						&& !( (OgmCollectionPersister) associatedJoinable ).getType().isEntityType() ) {
					// we have a collection of embedded
					// propertyAlias = createAliasForEmbedded( entityAlias, currentPropertyPath, optionalMatch );
					throw new NotYetImplementedException( "Collection of embedded not supported!" );
				}
				else {
					propertyEntityType = associationPropertyType.getAssociatedEntityName( getSessionFactory() );
					//log.infof( "propertyEntityType: %s", propertyEntityType );
					String targetNodeType = currentPersister.getEntityKeyMetadata().getTable();
					OgmEntityPersister associationPersister = getPersister( currentPersister.getEntityKeyMetadata().getTable() );
					log.infof( "targetNodeType: %s", targetNodeType );
					log.infof( "currentPersister: %s", currentPersister );
					log.infof( "associationPersister: %s", associationPersister );
					log.infof( "associationPropertyType: %s", associationPropertyType );
					log.infof( "associatedJoinable: %s", associatedJoinable );
					log.infof( "KeyColumnNames: %s", Arrays.asList( associatedJoinable.getKeyColumnNames() ) );
					log.infof( "associationPropertyType.useLHSPrimaryKey(): %s", associationPropertyType.useLHSPrimaryKey() );
					log.infof( "associationPropertyType.getLHSPropertyName(): %s", associationPropertyType.getLHSPropertyName() );
					log.infof( "associationPropertyType.getRHSUniqueKeyPropertyName(): %s", associationPropertyType.getRHSUniqueKeyPropertyName() );
					log.infof( "associatedJoinable.getKeyColumnNames(): %s", associatedJoinable.getKeyColumnNames() );

					if ( depth == requiredDepth ) {
						//this is search by key field

						isLastElementAssociation = true;
						break;
					}
					else {
						//need go deeper
						isLastElementAssociation = false;
						//entityAlias = findAliasForType( associationPropertyType.getAssociatedEntityName( sessionFactory ) );
						//log.infof( "entityAlias: %s", entityAlias );
						//propertyAlias = createAliasForAssociation( entityAlias, currentPropertyPath, targetNodeType);
						log.infof( "propertyAlias: %s", propertyAlias );
						lastAssociationPath = new ArrayList<>( currentPropertyPath );
						log.infof( "lastAssociationPath: %s", lastAssociationPath );
						isLastElementAssociation = false;
						log.infof( "isLastElementAssociation: %s", isLastElementAssociation );
						String associatedEntityName = associationPropertyType.getAssociatedEntityName( sessionFactory );
						//entityAlias = associatedEntityName.toLowerCase();
						//entityType = associatedEntityName;
						//propertyEntityType = associatedEntityName;
						propertyAlias = currentProperty;
						//currentPersister = getPersister( entityType );
						registerEntityAlias( associatedEntityName, propertyAlias );
					}

					//propertyName = currentProperty + "_" + associatedJoinable.getKeyColumnNames()[0];

					//isLastElementAssociation = true;
					//return new PropertyIdentifier( entityAlias, propertyName );
				}
			}
			else if ( currentPropertyType.isComponentType()
					&& !isIdProperty( currentPersister, propertyPath.subList( lastAssociationPath.size(), propertyPath.size() ) ) ) {
				// we are in the embedded case and the embedded is not the id of the entity (the id is stored as normal
				// properties)
				// propertyAlias = aliasResolver.createAliasForEmbedded( entityAlias, currentPropertyPath, optionalMatch
				// );
				throw new NotYetImplementedException( "Embedded not supported!" );
			}
			else {
				isLastElementAssociation = false;
			}
			depth++;
		}
		if ( isLastElementAssociation ) {
			// even the last element is an association, we need to find a suitable identifier property
			propertyName = getSessionFactory().getEntityPersister( propertyEntityType ).getIdentifierPropertyName();
		}
		else {
			// the last element is a property so we can build the test with this property
			propertyName = getColumnName( propertyEntityType, propertyPath.subList( lastAssociationPath.size(), propertyPath.size() ) );
		}
		log.infof( "PropertyIdentifier: %s", new PropertyIdentifier( propertyAlias, propertyName ) );
		return new PropertyIdentifier( propertyAlias, propertyName );
	}


	private boolean isReferenceToPrimaryKey(String propertyName, EntityType owningType) {
		EntityPersister persister = sessionFactory.getEntityPersister( owningType.getAssociatedEntityName() );
		if ( persister.getEntityMetamodel().hasNonIdentifierPropertyNamedId() ) {
			// only the identifier property field name can be a reference to the associated entity's PK...
			return propertyName.equals( persister.getIdentifierPropertyName() ) && owningType.isReferenceToPrimaryKey();
		}
		// here, we have two possibilities:
		// 1) the property-name matches the explicitly identifier property name
		// 2) the property-name matches the implicit 'id' property name
		// the referenced node text is the special 'id'
		if ( EntityPersister.ENTITY_ID.equals( propertyName ) ) {
			return owningType.isReferenceToPrimaryKey();
		}
		String keyPropertyName = sessionFactory.getIdentifierPropertyName( owningType.getName() );// .getIdentifierOrUniqueKeyPropertyName(
		// owningType );
		return keyPropertyName != null && keyPropertyName.equals( propertyName ) && owningType.isReferenceToPrimaryKey();
	}

	private String createAliasForAssociation(String entityAlias, List<String> propertyPathWithoutAlias, String targetEntityName) {
		log.debugf( "entityAlias:%s; propertyPathWithoutAlias:%s; targetEntityName:%s; needJoin:%s",
				entityAlias, propertyPathWithoutAlias, targetEntityName );
		RelationshipAliasTree relationshipAlias = relationshipAliases.get( entityAlias );
		log.debugf( "relationshipAlias:%s; entityAlias: %s", relationshipAlias, entityAlias );
		if ( relationshipAlias == null ) {
			relationshipAlias = RelationshipAliasTree.root( entityAlias );
			relationshipAliases.put( entityAlias, relationshipAlias );
		}
		log.infof( "relationshipAlias:%s; entityAlias: %s", relationshipAlias, entityAlias );
		for ( int i = 0; i < propertyPathWithoutAlias.size(); i++ ) {
			String name = propertyPathWithoutAlias.get( i );
			RelationshipAliasTree child = relationshipAlias.findChild( name );
			if ( child == null ) {
				if ( i != propertyPathWithoutAlias.size() - 1 ) {
					throw new AssertionFailure( "The path to " + StringHelper.join( propertyPathWithoutAlias, "." )
					+ " has not been completely constructed" );
				}

				relationshipCounter++;
				String childAlias = "_" + entityAlias + relationshipCounter;
				child = RelationshipAliasTree.relationship( childAlias, name, targetEntityName );
				log.infof( "child:%s; ", child );
				relationshipAlias.addChild( child );
			}
			relationshipAlias = child;
			String alias = relationshipAlias.getAlias();
			log.infof( "alias:%s; ", alias );
			/*	if ( optionalMatch && !requiredMatches.contains( alias ) ) {
				optionalMatches.add( alias );
			}
			else {
				requiredMatches.add( alias );
				optionalMatches.remove( alias );
			} */
		}
		return relationshipAlias.getAlias();
	}

	public String getColumnName(String entityType, List<String> propertyPathWithoutAlias) {
		return getColumnName( getPersister( entityType ), propertyPathWithoutAlias );
	}

	public String getColumnName(Class<?> entityType, List<String> propertyName) {
		OgmEntityPersister persister = (OgmEntityPersister) getSessionFactory().getEntityPersister( entityType.getName() );
		return getColumnName( persister, propertyName );
	}

	private String getColumnName(OgmEntityPersister persister, List<String> propertyPathWithoutAlias) {
		if ( isIdProperty( persister, propertyPathWithoutAlias ) ) {
			return "_KEY"; // getColumn( persister, propertyPathWithoutAlias );
		}
		String columnName = getColumn( persister, propertyPathWithoutAlias );
		if ( isNestedProperty( propertyPathWithoutAlias ) ) {
			columnName = columnName.substring( columnName.lastIndexOf( '.' ) + 1, columnName.length() );
		}
		return columnName;
	}

	public boolean isIdProperty(String entityType, List<String> propertyPath) {
		return isIdProperty( getPersister( entityType ), propertyPath );
	}

	/**
	 * Check if the property is part of the identifier of the entity.
	 *
	 * @param persister the {@link OgmEntityPersister} of the entity with the property
	 * @param namesWithoutAlias the path to the property with all the aliases resolved
	 * @return {@code true} if the property is part of the id, {@code false} otherwise.
	 */
	public boolean isIdProperty(OgmEntityPersister persister, List<String> namesWithoutAlias) {
		String join = StringHelper.join( namesWithoutAlias, "." );
		Type propertyType = persister.getPropertyType( namesWithoutAlias.get( 0 ) );
		String[] identifierColumnNames = persister.getIdentifierColumnNames();
		if ( propertyType.isComponentType() ) {
			String[] embeddedColumnNames = persister.getPropertyColumnNames( join );
			for ( String embeddedColumn : embeddedColumnNames ) {
				if ( !ArrayHelper.contains( identifierColumnNames, embeddedColumn ) ) {
					return false;
				}
			}
			return true;
		}
		return ArrayHelper.contains( identifierColumnNames, join );
	}

	public EntityKeyMetadata getKeyMetaData(String entityType) {
		OgmEntityPersister persister = (OgmEntityPersister) getSessionFactory().getEntityPersister( entityType );
		return persister.getEntityKeyMetadata();
	}

	/**
	 * Checks whether the supplied character is a letter.
	 */
	private boolean isLetter(int c) {
		return isUpperCaseLetter( c ) || isLowerCaseLetter( c );
	}
	/**
	 * Checks whether the supplied character is an upper-case letter.
	 */
	private boolean isUpperCaseLetter(int c) {
		return ( c >= 65 && c <= 90 ); // A - Z
	}
	/**
	 * Checks whether the supplied character is an lower-case letter.
	 */
	private boolean isLowerCaseLetter(int c) {
		return ( c >= 97 && c <= 122 ); // a - z
	}
	/**
	 * Checks whether the supplied character is a number
	 */
	private boolean isNumber(int c) {
		return ( c >= 48 && c <= 57 ); // 0 - 9
	}

	public void registerEntityAlias(String entityName, String alias) {
		log.infof( "entityName: %s, alias:%s",entityName, alias );
		StringBuilder sb = new StringBuilder( alias );
		for ( int i = 0; i < sb.length(); i++ ) {
			char c = sb.charAt( i );
			if ( c == '_' || isLetter( c ) ||  ( i > 0 && isNumber( c ) ) ) {
				continue;
			}
			sb.setCharAt( i, '_' );
		}
		aliasByEntityName.put( entityName, sb.toString() );
	}

	public String findAliasForType(String entityType) {
		return aliasByEntityName.get( entityType );
	}

	public List<String> getTypes() {
		return new ArrayList<>( aliasByEntityName.keySet() );
	}
}
