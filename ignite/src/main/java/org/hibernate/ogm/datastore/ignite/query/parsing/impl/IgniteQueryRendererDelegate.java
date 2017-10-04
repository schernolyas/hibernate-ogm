/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.ignite.query.parsing.impl;

import static org.hibernate.engine.internal.JoinHelper.getLHSColumnNames;
import static org.hibernate.engine.internal.JoinHelper.getLHSTableName;
import static org.hibernate.engine.internal.JoinHelper.getRHSColumnNames;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.ast.common.JoinType;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.hibernate.hql.ast.spi.SingleEntityQueryRendererDelegate;
import org.hibernate.ogm.datastore.ignite.logging.impl.Log;
import org.hibernate.ogm.datastore.ignite.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.ignite.query.impl.IgniteQueryDescriptor;
import org.hibernate.ogm.datastore.ignite.query.parsing.predicate.impl.IgnitePredicateFactory;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.persister.entity.Joinable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.persister.walking.spi.AssociationKey;
import org.hibernate.tuple.NonIdentifierAttribute;
import org.hibernate.tuple.entity.EntityBasedAssociationAttribute;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.ForeignKeyDirection;

import org.antlr.runtime.tree.Tree;

/**
 * Parser delegate which creates Ignite SQL queries in form of {@link StringBuilder}s.
 *
 * @author Victor Kadachigov
 */
public class IgniteQueryRendererDelegate extends SingleEntityQueryRendererDelegate<StringBuilder, IgniteQueryParsingResult> {

	private static final List<String> ENTITY_COLUMN_NAMES = Collections.unmodifiableList( Arrays.asList( "_KEY", "_VALUE" ) );
	private static final Log LOG = LoggerFactory.getLogger();
	private final IgnitePropertyHelper propertyHelper;
	private final SessionFactoryImplementor sessionFactory;
	private final Map<String, Object> namedParamsWithValues;
	private List<Object> indexedParameters;
	private List<OrderByClause> orderByExpressions;

	private JoinType joinType;

	public IgniteQueryRendererDelegate(SessionFactoryImplementor sessionFactory, IgnitePropertyHelper propertyHelper, EntityNamesResolver entityNamesResolver, Map<String, Object> namedParameters) {
		super(
				propertyHelper,
				entityNamesResolver,
				SingleEntityQueryBuilder.getInstance( new IgnitePredicateFactory( propertyHelper ), propertyHelper ),
				namedParameters != null ? NamedParametersMap.INSTANCE : null /* we put '?' in query instead of parameter value */
		);
		this.propertyHelper = propertyHelper;
		this.sessionFactory = sessionFactory;
		this.namedParamsWithValues = namedParameters;
	}

	@Override
	public void setPropertyPath(PropertyPath propertyPath) {
		LOG.infof( "=======propertyPath: %s",propertyPath );
		this.propertyPath = propertyPath;
	}

	private void where( StringBuilder queryBuilder ) {
		boolean needJoin = propertyHelper.getTypes().size()>1;
		StringBuilder where = builder.build();
		if ( where != null && where.length() > 0 ) {
			queryBuilder.append( " WHERE " ).append( where );
			if ( needJoin ) {
				queryBuilder.append( " AND " );
			}
		}
		//add join info
		if ( needJoin ) {
			// alias corresponds to property
			OgmEntityPersister targetTypePersister = (OgmEntityPersister) ( sessionFactory ).getEntityPersister( targetTypeName );
			String targetAlilas = propertyHelper.findAliasForType( targetTypeName );
			for ( Iterator<String> it = propertyHelper.getTypes().iterator(); it.hasNext(); ) {
				String currentTypeName = it.next();
				if ( currentTypeName.equals( targetTypeName ) ) {
					continue;
				}
				OgmEntityPersister currentTypePersister = (OgmEntityPersister) ( sessionFactory ).getEntityPersister( currentTypeName );
				//we have a join
				String joinTable = getTableName( currentTypeName );
				String joinAlias = propertyHelper.findAliasForType( currentTypeName );
				queryBuilder.append( targetAlilas );
				EntityMetamodel targetEntityMetamodel = targetTypePersister.getEntityMetamodel();
				EntityBasedAssociationAttribute targetAssociationAttr = (EntityBasedAssociationAttribute) targetEntityMetamodel.getProperties()[targetEntityMetamodel.getPropertyIndex( joinAlias )];
				LOG.infof( "=======targetAssociationAttr: %s; class: %s",targetAssociationAttr, targetAssociationAttr.getClass() );
				LOG.infof( "=======targetAssociationAttr.getAssociationKey: %s",targetAssociationAttr.getAssociationKey() );
				final Joinable joinable = targetAssociationAttr.getType().getAssociatedJoinable( sessionFactory );
				LOG.infof( "=======targetAssociationAttr.getType().getForeignKeyDirection(): %s",targetAssociationAttr.getType().getForeignKeyDirection() );
				if ( targetAssociationAttr.getType().getForeignKeyDirection() == ForeignKeyDirection.FROM_PARENT ) {
					final String lhsTableName;
					final String[] lhsColumnNames;
					LOG.infof( "=======joinable.isCollection(): %s",joinable.isCollection() );


					if ( joinable.isCollection() ) {
						final QueryableCollection collectionPersister = (QueryableCollection) joinable;
						lhsTableName = collectionPersister.getTableName();
						lhsColumnNames = collectionPersister.getElementColumnNames();
						LOG.infof( "=======lhsTableName: %s, lhsColumnNames:%sl index columns: %s",
								   lhsTableName, lhsColumnNames,collectionPersister.getIndexColumnNames() );
						queryBuilder.append( "." ).append( lhsColumnNames[0] ).append( "=" );
					}
					else {
						final OuterJoinLoadable entityPersister = (OuterJoinLoadable) targetAssociationAttr.getSource();
						lhsTableName = getLHSTableName( targetAssociationAttr.getType(), targetEntityMetamodel.getPropertyIndex( joinAlias ), entityPersister );
						lhsColumnNames = getLHSColumnNames( targetAssociationAttr.getType(), targetEntityMetamodel.getPropertyIndex( joinAlias ), entityPersister, sessionFactory );
						String[] rhsColumnNames=getRHSColumnNames( targetAssociationAttr.getType(),sessionFactory  );
						LOG.infof( "=======lhsColumnNames: %s ;rhs: %s",
								   lhsColumnNames,rhsColumnNames );
						queryBuilder.append( "." ).append( lhsColumnNames[0] ).append( "=" ).append( joinAlias ).append( "." ).append( rhsColumnNames[0] );
					}
					//return new AssociationKey( lhsTableName, lhsColumnNames );
					//LOG.infof( "=======lhsTableName: %s, lhsColumnNames:%s",lhsTableName, lhsColumnNames );
					//queryBuilder.append( "." ).append( lhsColumnNames[0] ).append( "=" );


				} /*
				else {
					LOG.infof( "=======lhsTableName: %s, lhsColumnNames:%s",joinable.getTableName(), getRHSColumnNames( attr.getType(), sessionFactory ) );
				} */



				//attr.getType().getName()
			}

		}
	}

	private void orderBy(StringBuilder queryBuilder) {
		if ( orderByExpressions != null && !orderByExpressions.isEmpty() ) {
			queryBuilder.append( " ORDER BY " );
			int counter = 1;
			for ( OrderByClause orderBy : orderByExpressions ) {
				orderBy.asString( queryBuilder );
				if ( counter++ < orderByExpressions.size() ) {
					queryBuilder.append( ", " );
				}
			}
		}
	}

	private void select(StringBuilder queryBuilder) {
		String tableAlias = propertyHelper.findAliasForType( targetTypeName );
		if ( tableAlias.trim().length() > 0 ) {
			tableAlias = tableAlias + ".";
		}
		queryBuilder.append( String.format( "SELECT %s_KEY, %s_VAL ", tableAlias, tableAlias ) );
	}

	private void from(StringBuilder queryBuilder) {
		LOG.infof( "propertyHelper.getTypes():%s",propertyHelper.getTypes() );
		queryBuilder.append( " FROM " );

		String tableAlias = propertyHelper.findAliasForType( targetTypeName );
		String tableName = getTableName( targetTypeName );
		queryBuilder.append( tableName ).append( ' ' ).append( tableAlias ).append( ' ' );
		for ( String currentTypeName : propertyHelper.getTypes() ) {
			if ( currentTypeName.equals( targetTypeName ) ) {
				continue;
			}
			//we have a join
			String joinTable = getTableName( currentTypeName );
			String joinAlias = propertyHelper.findAliasForType( currentTypeName );
			queryBuilder.append( " , " ).append( joinTable ).append( ' ' ).append( joinAlias );
		}
	}

	private String getTableName(String typeName) {
		LOG.infof( "typeName:%s",typeName );
		String tableAlias = propertyHelper.findAliasForType( typeName );
		OgmEntityPersister persister = (OgmEntityPersister) ( sessionFactory ).getEntityPersister( typeName );
		return propertyHelper.getKeyMetaData( typeName ).getTable();
	}

	@Override
	public IgniteQueryParsingResult getResult() {
		LOG.info( "===getResult===" );
		StringBuilder queryBuilder = new StringBuilder();
		select( queryBuilder );
		from( queryBuilder );
		where( queryBuilder );
		orderBy( queryBuilder );

		boolean hasScalar = false; // no projections for now
		IgniteQueryDescriptor queryDescriptor = new IgniteQueryDescriptor( queryBuilder.toString(),
																		   getTableName( targetTypeName ),
																		   indexedParameters, hasScalar,(propertyHelper.getTypes().size()>1)
		);

		return new IgniteQueryParsingResult( queryDescriptor, ENTITY_COLUMN_NAMES );
	}

	@Override
	protected void addSortField(PropertyPath propertyPath, String collateName, boolean isAscending) {
		LOG.infof( "propertyPath: %s,collateName:%s,isAscending:%s",propertyPath,collateName,isAscending );
		if ( orderByExpressions == null ) {
			orderByExpressions = new ArrayList<OrderByClause>();
		}

		List<String> propertyPathWithoutAlias = resolveAlias( propertyPath );
		PropertyIdentifier identifier = propertyHelper.getPropertyIdentifier( targetTypeName, propertyPathWithoutAlias, 0 );

		OrderByClause order = new OrderByClause( identifier.getAlias(), identifier.getPropertyName(), isAscending );
		orderByExpressions.add( order );
	}

	@Override
	public void pushFromStrategy(JoinType joinType, Tree associationFetchTree, Tree propertyFetchTree, Tree alias) {
		super.pushFromStrategy( joinType, associationFetchTree, propertyFetchTree, alias );
		this.joinType = joinType;
	}

	@Override
	public void popStrategy() {
		super.popStrategy();
		joinType = null;
	}

	@Override
	public void registerJoinAlias(Tree alias, PropertyPath path) {
		LOG.infof( "alias: %s ; path: %s",alias,path );
		super.registerJoinAlias( alias, path );
		List<String> propertyPath = resolveAlias( path );

		int requiredDepth;
		// For now, we deal with INNER JOIN and LEFT OUTER JOIN, it's not really perfect as you might have issues
		// with join precedence but it's probably the best we can do for now.
		if ( JoinType.INNER.equals( joinType ) ) {
			requiredDepth = propertyPath.size();
		}
		else if ( JoinType.LEFT.equals( joinType ) ) {
			requiredDepth = 0;
		}
		else {
			LOG.joinTypeNotFullySupported( joinType );
			// defaults to mark the alias as required for now
			requiredDepth = propertyPath.size();
		}

		// Even if we don't need the property identifier, it's important to create the aliases for the corresponding
		// associations/embedded with the correct requiredDepth.
		propertyHelper.getPropertyIdentifier( targetTypeName, propertyPath, requiredDepth );
	}

	private void fillIndexedParams(String param) {
		if ( param.startsWith( ":" ) ) {
			if ( indexedParameters == null ) {
				indexedParameters = new ArrayList<>();
			}
			Object paramValue = namedParamsWithValues.get( param.substring( 1 ) );
			if ( paramValue != null && paramValue.getClass().isEnum() ) {
				//vk: for now I work only with @Enumerated(EnumType.ORDINAL) field params
				//    How to determite corresponding field to this param and check it's annotation?
				paramValue = ( (Enum) paramValue ).ordinal();
			}
			indexedParameters.add( paramValue );
		}
	}

	@Override
	public void predicateLess(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateLess( comparativePredicate );
	}

	@Override
	public void predicateLessOrEqual(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateLessOrEqual( comparativePredicate );
	}

	@Override
	public void predicateEquals(final String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateEquals( comparativePredicate );
	}

	@Override
	public void predicateNotEquals(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateNotEquals( comparativePredicate );
	}

	@Override
	public void predicateGreaterOrEqual(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateGreaterOrEqual( comparativePredicate );
	}

	@Override
	public void predicateGreater(String comparativePredicate) {
		fillIndexedParams( comparativePredicate );
		super.predicateGreater( comparativePredicate );
	}

	@Override
	public void predicateBetween(String lower, String upper) {
		fillIndexedParams( lower );
		fillIndexedParams( upper );
		super.predicateBetween( lower, upper );
	}

	@Override
	public void predicateLike(String patternValue, Character escapeCharacter) {
		fillIndexedParams( patternValue );
		super.predicateLike( patternValue, escapeCharacter );
	}

	@Override
	public void predicateIn(List<String> list) {
		for ( String s : list ) {
			fillIndexedParams( s );
		}
		super.predicateIn( list );
	}

	private static class NamedParametersMap implements Map<String, Object> {

		public static final NamedParametersMap INSTANCE = new NamedParametersMap();

		@Override
		public int size() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public boolean isEmpty() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public boolean containsKey(Object key) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public boolean containsValue(Object value) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Object get(Object key) {
			return PropertyIdentifier.PARAM_INSTANCE;
		}
		@Override
		public Object put(String key, Object value) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Object remove(Object key) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public void putAll(Map<? extends String, ? extends Object> m) {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public void clear() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Set<String> keySet() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Collection<Object> values() {
			throw new UnsupportedOperationException( "Not supported" );
		}
		@Override
		public Set<java.util.Map.Entry<String, Object>> entrySet() {
			throw new UnsupportedOperationException( "Not supported" );
		}
	}

}
