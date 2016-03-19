/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.dto.EmbeddedColumnInfo;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.datastore.ogm.orientdb.utils.SequenceUtil;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.Value;
import org.hibernate.ogm.datastore.spi.BaseSchemaDefiner;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.type.BigDecimalType;
import org.hibernate.type.BigIntegerType;
import org.hibernate.type.BinaryType;
import org.hibernate.type.BooleanType;
import org.hibernate.type.ByteType;
import org.hibernate.type.CalendarDateType;
import org.hibernate.type.CalendarType;
import org.hibernate.type.CharacterType;
import org.hibernate.type.CustomType;
import org.hibernate.type.DateType;
import org.hibernate.type.DoubleType;
import org.hibernate.type.EntityType;
import org.hibernate.type.EnumType;
import org.hibernate.type.FloatType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.ManyToOneType;
import org.hibernate.type.MaterializedBlobType;
import org.hibernate.type.MaterializedClobType;
import org.hibernate.type.NumericBooleanType;
import org.hibernate.type.OneToOneType;
import org.hibernate.type.SerializableToBlobType;
import org.hibernate.type.ShortType;
import org.hibernate.type.StringType;
import org.hibernate.type.TimeType;
import org.hibernate.type.TimestampType;
import org.hibernate.type.TrueFalseType;
import org.hibernate.type.UUIDBinaryType;
import org.hibernate.type.UrlType;
import org.hibernate.type.YesNoType;
import org.hibernate.usertype.UserType;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class OrientDBSchemaDefiner extends BaseSchemaDefiner {

	private static final String CREATE_PROPERTY_TEMPLATE = "create property {0}.{1} {2}";
	private static final String CREATE_EMBEDDED_PROPERTY_TEMPLATE = "create property {0}.{1} {2} {3}";
	private static final Log log = LoggerFactory.getLogger();
	private static final Pattern PATTERN = Pattern.compile( "directed([a-zA-Z_0-9])\\.(.+)" );
	private static final Set<Class> RELATIONS_TYPES;
	private static final Map<Class, Class> RETURNED_CLASS_TYPE_MAPPING;

	private static final Set<Class> SEQ_TYPES;

	private static final Map<Class, String> TYPE_MAPPING;

	private OrientDBDatastoreProvider provider;

	static {
		Map<Class, String> map = new HashMap<>();

		map.put( ByteType.class, "byte" );
		map.put( IntegerType.class, "integer" );
		map.put( NumericBooleanType.class, "short" );
		map.put( ShortType.class, "short" );
		map.put( LongType.class, "long" );
		map.put( FloatType.class, "float" );
		map.put( DoubleType.class, "double" );
		map.put( DateType.class, "date" );
		map.put( CalendarDateType.class, "date" );
		map.put( TimestampType.class, "datetime" );
		map.put( CalendarType.class, "datetime" );
		map.put( TimeType.class, "datetime" );

		map.put( BooleanType.class, "boolean" );

		map.put( TrueFalseType.class, "string" );
		map.put( YesNoType.class, "string" );
		map.put( StringType.class, "string" );
		map.put( UrlType.class, "string" );

		map.put( CharacterType.class, "string" );
		map.put( UUIDBinaryType.class, "string" );

		map.put( BinaryType.class, "binary" ); // byte[]
		map.put( MaterializedBlobType.class, "binary" ); // byte[]
		map.put( SerializableToBlobType.class, "binary" ); // byte[]
		map.put( BigIntegerType.class, "binary" );
		map.put( MaterializedClobType.class, "binary" );

		map.put( BigDecimalType.class, "decimal" );

		TYPE_MAPPING = Collections.unmodifiableMap( map );

		Map<Class, Class> map1 = new HashMap<>();
		map1.put( Long.class, LongType.class );
		map1.put( Integer.class, IntegerType.class );
		map1.put( String.class, StringType.class );
		RETURNED_CLASS_TYPE_MAPPING = Collections.unmodifiableMap( map1 );

		Set<Class> set1 = new HashSet<>();
		set1.add( IntegerType.class );
		set1.add( LongType.class );
		SEQ_TYPES = Collections.unmodifiableSet( set1 );

		Set<Class> set2 = new HashSet<>();
		set2.add( ManyToOneType.class );
		set2.add( OneToOneType.class );
		RELATIONS_TYPES = Collections.unmodifiableSet( set2 );

	}

	private String createClassQuery(String tableName) {
		return String.format( "create class %s extends V", tableName );
	}

	private String createClassQuery(Table table) {
		return createClassQuery( table.getName() );
	}

	private void createEntities(SchemaDefinitionContext context) {
		// check exists sequence
		try {
			log.debugf( "default hibernate sequence value: %s", SequenceUtil.getSequence( provider.getConnection(), "hibernate_sequence" ) );
		}
		catch (HibernateException he) {
			// no sequence. try to create it
			try {
				provider.getConnection().createStatement().execute( "CREATE SEQUENCE hibernate_sequence TYPE ORDERED START 0" );
			}
			catch (SQLException e) {
				throw log.cannotGenerateSequence( "hibernate_sequence", e );
			}
		}

		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			for ( Table table : namespace.getTables() ) {
				log.debugf( "table: %s", table );
				boolean isMappingTable = isMapingTable( table );
				String classQuery = createClassQuery( table );
				log.debugf( "create class query: %s", classQuery );
				try {
					provider.getConnection().createStatement().execute( classQuery );
				}
				catch (SQLException e) {
					throw log.cannotGenerateVertexClass( table.getName(), e );
				}
				Iterator<Column> columnIterator = table.getColumnIterator();
				Set<String> embeddedClassNames = new HashSet<>();

				while ( columnIterator.hasNext() ) {
					Column column = columnIterator.next();
					log.debugf( "column: %s ", column );
					log.debugf( "relation type: %s", column.getValue().getType().getClass() );

					if ( OrientDBConstant.SYSTEM_FIELDS.contains( column.getName() ) ) {
						continue;
					}
					else if ( RELATIONS_TYPES.contains( column.getValue().getType().getClass() ) ) {
						// @TODO refactor it
						Value value = column.getValue();
						log.debugf( "column name: %s ; column.getCanonicalName(): %s", column.getName(), column.getCanonicalName() );
						if ( EntityKeyUtil.isEmbeddedColumn( column ) ) {
							EmbeddedColumnInfo ec = new EmbeddedColumnInfo( column.getName() );
							log.debugf( "embedded column. class: %s ; property: %s", ec.getClassName(), ec.getPropertyName() );

						}
						else {
							Class mappedByClass = searchMappedByReturnedClass( context, namespace.getTables(), (EntityType) value.getType(), column );
							String propertyQuery = createValueProperyQuery( table, column, RETURNED_CLASS_TYPE_MAPPING.get( mappedByClass ) );
							log.debugf( "create foreign key property query: %s", propertyQuery );
							try {
								provider.getConnection().createStatement().execute( propertyQuery );
							}
							catch (SQLException e) {
								throw log.cannotGenerateProperty( column.getName(), table.getName(), e );
							}
						}
						// @TODO use Links as foreign keys. see http://orientdb.com/docs/last/SQL-Create-Link.html

					}
					else if ( EntityKeyUtil.isEmbeddedColumn( column ) ) {
						EmbeddedColumnInfo ec = new EmbeddedColumnInfo( column.getName() );
						log.debugf( "embedded column. class: %s ; property: %s", ec.getClassName(), ec.getPropertyName() );
						if ( embeddedClassNames.contains( ec.getClassName() ) ) {
							// update embedded class
							String propertyQuery = createValueProperyQuery( table, column );
							log.debug( "create embedded property query: " + propertyQuery );
							try {
								provider.getConnection().createStatement().execute( propertyQuery );
							}
							catch (SQLException e) {
								throw log.cannotGenerateProperty( ec.getPropertyName(), ec.getClassName(), e );
							}
						}
						else {
							String executedQuery = null;
							try {
								executedQuery = createClassQuery( ec.getClassName() );
								embeddedClassNames.add( ec.getClassName() );
								log.debugf( "create embedded class query: %s", executedQuery );
								provider.getConnection().createStatement().execute( executedQuery );
								executedQuery = createEmbeddedProperyQuery( table, column );
								log.debugf( "create property for embedded class query: %s", executedQuery );
								provider.getConnection().createStatement().execute( executedQuery );
								executedQuery = createValueProperyQuery( table, column );
								log.debug( "create embedded property query: " + executedQuery );
								provider.getConnection().createStatement().execute( executedQuery );
							}
							catch (SQLException e) {
								throw log.cannotGenerateVertexClass( executedQuery, e );
							}
						}

					}
					else {
						String propertyQuery = createValueProperyQuery( table, column );
						log.debug( "create property query: " + propertyQuery );
						try {
							provider.getConnection().createStatement().execute( propertyQuery );
						}
						catch (SQLException e) {
							throw log.cannotGenerateProperty( column.getName(), table.getName(), e );
						}
					}
				}
				if ( !isMappingTable ) {
					PrimaryKey primaryKey = table.getPrimaryKey();
					if ( primaryKey != null ) {
						log.debugf( "primaryKey: %s ", primaryKey );
						createPrimaryKey( provider.getConnection(), primaryKey );
					}
					else {
						log.debugf( "Table %s has not primary key", table.getName() );
					}
				}
			}
		}

	}

	private void createPrimaryKey(Connection connection, PrimaryKey primaryKey) {
		String table = primaryKey.getTable().getName();
		StringBuilder uniqueIndexQuery = new StringBuilder( 100 );
		uniqueIndexQuery.append( "CREATE INDEX " ).append( table ).append( "_" );
		String firstColumn = primaryKey.getColumn( 0 ).getName();
		uniqueIndexQuery.append( firstColumn ).append( "_pk ON " ).append( table ).append( " (" ).append( firstColumn ).append( ") UNIQUE" );

		try {
			log.debugf( "query: %s", uniqueIndexQuery );
			connection.createStatement().execute( uniqueIndexQuery.toString() );
		}
		catch (SQLException e) {
			throw log.cannotExecuteQuery( uniqueIndexQuery.toString(), e );
		}
		StringBuilder seq = new StringBuilder( 100 );
		if ( primaryKey.getColumns().size() == 1 && SEQ_TYPES.contains( primaryKey.getColumns().get( 0 ).getValue().getType().getClass() ) ) {
			seq.append( "CREATE SEQUENCE " );
			seq.append( generateSeqName( primaryKey.getTable().getName(), primaryKey.getColumns().get( 0 ).getName() ) );
			seq.append( " TYPE ORDERED START 0" );
			try {
				log.debugf( "query: %s", seq );
				connection.createStatement().execute( seq.toString() );
			}
			catch (SQLException e) {
				throw log.cannotExecuteQuery( seq.toString(), e );
			}
		}
	}

	private String createValueProperyQuery(Table table, Column column) {
		SimpleValue simpleValue = (SimpleValue) column.getValue();
		return createValueProperyQuery( table, column, simpleValue.getType().getClass() );
	}

	private String createEmbeddedProperyQuery(Table table, Column column) {
		EmbeddedColumnInfo ec = new EmbeddedColumnInfo( column.getName() );
		return MessageFormat.format( CREATE_EMBEDDED_PROPERTY_TEMPLATE,
				table.getName(), ec.getClassName(), "embedded", ec.getClassName() );
	}

	private String createValueProperyQuery(Table table, Column column, Class targetTypeClass) {

		Value value = column.getValue();
		log.debugf( "1.Column: %s, targetTypeClass: %s ", column.getName(), targetTypeClass );
		String query = null;

		if ( targetTypeClass.equals( CustomType.class ) ) {
			CustomType type = (CustomType) value.getType();
			log.debug( "2.Column " + column.getName() + " :" + type.getUserType() );
			UserType userType = type.getUserType();
			if ( userType instanceof EnumType ) {
				EnumType enumType = (EnumType) type.getUserType();
				query = MessageFormat.format( CREATE_PROPERTY_TEMPLATE,
						table.getName(), column.getName(), TYPE_MAPPING.get( enumType.isOrdinal() ? IntegerType.class : StringType.class ) );

			}
			else {
				throw new UnsupportedOperationException( "Unsupported user type: " + userType.getClass() );
			}
		}
		else {
			String orientDbTypeName = TYPE_MAPPING.get( targetTypeClass );
			if ( orientDbTypeName == null ) {
				throw new UnsupportedOperationException( "Unsupported type: " + targetTypeClass );
			}
			if ( EntityKeyUtil.isEmbeddedColumn( column ) ) {
				EmbeddedColumnInfo ec = new EmbeddedColumnInfo( column.getName() );
				query = MessageFormat.format( CREATE_PROPERTY_TEMPLATE,
						ec.getClassName(), ec.getPropertyName(), orientDbTypeName );
			}
			else {
				query = MessageFormat.format( CREATE_PROPERTY_TEMPLATE,
						table.getName(), column.getName(), orientDbTypeName );
			}

		}
		return query;

	}

	private boolean isMapingTable(Table table) {
		int tableColumns = 0;
		for ( Iterator iterator = table.getColumnIterator(); iterator.hasNext(); ) {
			Object next = iterator.next();
			tableColumns++;
		}
		return table.getPrimaryKey() == null && tableColumns == 2;
	}

	/*
	 * private EmbedColumnName prepareColumnNames(Column column) { EmbedColumnName names = null; String columnName =
	 * column.getName(); Matcher matcher = PATTERN.matcher( columnName ); if ( matcher.find() ) { String mainFieldName =
	 * matcher.group( 1 ); String emdeddedFieldName = matcher.group( 2 ); names = new EmbedColumnName( mainFieldName,
	 * emdeddedFieldName ); } return names; }
	 */

	private String searchMappedByName(SchemaDefinitionContext context, Collection<Table> tables, EntityType type, Column currentColumn) {
		String columnName = currentColumn.getName();
		String tableName = type.getAssociatedJoinable( context.getSessionFactory() ).getTableName();

		String primaryKeyName = null;
		for ( Table table : tables ) {
			if ( table.getName().equals( tableName ) ) {
				primaryKeyName = table.getPrimaryKey().getColumn( 0 ).getName();
			}
		}
		return columnName.replace( "_" + primaryKeyName, "" );

	}

	private Class searchMappedByReturnedClass(SchemaDefinitionContext context, Collection<Table> tables, EntityType type, Column currentColumn) {
		String tableName = type.getAssociatedJoinable( context.getSessionFactory() ).getTableName();

		Class primaryKeyClass = null;
		for ( Table table : tables ) {
			if ( table.getName().equals( tableName ) ) {
				log.debug( "primary key type: " + table.getPrimaryKey().getColumn( 0 ).getValue().getType().getReturnedClass() );
				primaryKeyClass = table.getPrimaryKey().getColumn( 0 ).getValue().getType().getReturnedClass();
			}
		}
		return primaryKeyClass;
	}

	@Override
	public void initializeSchema(SchemaDefinitionContext context) {
		log.debug( "initializeSchema" );
		SessionFactoryImplementor sessionFactoryImplementor = context.getSessionFactory();
		ServiceRegistryImplementor registry = sessionFactoryImplementor.getServiceRegistry();
		provider = (OrientDBDatastoreProvider) registry.getService( DatastoreProvider.class );
		createEntities( context );
	}

	@Override
	public void validateMapping(SchemaDefinitionContext context) {
		log.debug( "validateMapping" );
		super.validateMapping( context );
	}

	public static String generateSeqName(String tableName, String primaryKeyName) {
		StringBuilder buffer = new StringBuilder();
		buffer.append( "seq_" ).append( tableName.toLowerCase() ).append( "_" ).append( primaryKeyName.toLowerCase() );
		return buffer.toString();
	}

}
