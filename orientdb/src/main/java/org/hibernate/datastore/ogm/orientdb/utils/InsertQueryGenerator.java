/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.DatatypeConverter;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.dto.EmbeddedColumnInfo;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.spi.Tuple;
import org.json.simple.JSONObject;

/**
 * Util class for generate query for insert entity
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class InsertQueryGenerator {

	private static final Log log = LoggerFactory.getLogger();
	private static final Set<Class> PREPARED_STATEMENT_TYPES;

	private static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {

		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat( OrientDBConstant.DATETIME_FORMAT );
		}
	};

	static {
		Set<Class> set = new HashSet<>();
		set.add( BigInteger.class );
		set.add( byte[].class );
		set.add( BigDecimal.class );
		PREPARED_STATEMENT_TYPES = Collections.unmodifiableSet( set );
	}

	public GenerationResult generate(String tableName, Tuple tuple) {
		return generate( tableName, TupleUtil.toMap( tuple ) );
	}

	public GenerationResult generate(String tableName, Map<String, Object> valuesMap) {
		QueryResult queryResult = createJSON( valuesMap );
		StringBuilder insertQuery = new StringBuilder( 100 );
		insertQuery.append( "insert into " ).append( tableName ).append( " content " ).append( queryResult.getJson().toJSONString() );
		return new GenerationResult( queryResult.getPreparedStatementParams(), insertQuery.toString() );
	}

	protected QueryResult createJSON(Map<String, Object> valuesMap) {
		QueryResult result = new QueryResult();
		for ( Map.Entry<String, Object> entry : valuesMap.entrySet() ) {
			String columnName = entry.getKey();
			Object columnValue = entry.getValue();
			if ( OrientDBConstant.SYSTEM_FIELDS.contains( columnName ) ||  OrientDBConstant.MAPPING_FIELDS.containsKey( columnName )) {
				continue;
			}
			log.debugf( "createJSON: Column %s; value: %s (class: %s) ", columnName, columnValue, ( columnValue != null ? columnValue.getClass() : null ) );
			if ( EntityKeyUtil.isEmbeddedColumn( columnName ) ) {
				EmbeddedColumnInfo ec = new EmbeddedColumnInfo( columnName );
				if ( !result.getJson().containsKey( ec.getClassNames().get( 0 ) ) ) {
					JSONObject embeddedFieldValue = createDefaultEmbeddedRow( ec.getClassNames().get( 0 ) );
					result.getJson().put( ec.getClassNames().get( 0 ), embeddedFieldValue );
				}
				setJsonValue( result, ec, columnValue );
			}
			else if ( PREPARED_STATEMENT_TYPES.contains( columnValue.getClass() ) ) {
				result.getJson().put( columnName, "?" );
				if ( columnValue instanceof BigInteger ) {
                                        result.getJson().remove(columnName );
                                        result.getJson().put( columnName, DatatypeConverter.printBase64Binary( ( (BigInteger) columnValue ).toByteArray())  );
				} else if ( columnValue instanceof byte[] ) {
                                        result.getJson().remove(columnName );
                                        result.getJson().put( columnName, DatatypeConverter.printBase64Binary( (byte[]) columnValue)  );
                                } 
				else {
					result.getPreparedStatementParams().add( columnValue );
				}
			}
			else if ( columnValue instanceof Date || columnValue instanceof Calendar ) {
				Calendar calendar = null;
				if ( columnValue instanceof Date ) {
					calendar = Calendar.getInstance();
					calendar.setTime( (Date) columnValue );
				}
				else if ( columnValue instanceof Calendar ) {
					calendar = (Calendar) columnValue;
				}
				String formattedStr = ( FORMATTER.get() ).format( calendar.getTime() );
				result.getJson().put( columnName, formattedStr );
			}
			else if ( columnValue instanceof Character ) {
				result.getJson().put( columnName, ( (Character) columnValue ).toString() );
			}                        
			else {
				result.getJson().put( columnName, columnValue );
			}
		}
		return result;
	}

	private void setJsonValue(QueryResult result, EmbeddedColumnInfo ec, Object value) {
		JSONObject json = (JSONObject) result.getJson().get( ec.getClassNames().get( 0 ) );
		for ( int i = 1; i < ec.getClassNames().size(); i++ ) {
			if ( !json.containsKey( ec.getClassNames().get( i ) ) ) {
				json.put( ec.getClassNames().get( i ), createDefaultEmbeddedRow( ec.getClassNames().get( i ) ) );
			}
			json = (JSONObject) json.get( ec.getClassNames().get( i ) );
		}
		json.put( ec.getPropertyName(), value );
	}

	private JSONObject createDefaultEmbeddedRow(String className) {
		JSONObject embeddedFieldValue = new JSONObject();
                embeddedFieldValue.put( "@type", "d" );
		embeddedFieldValue.put( "@class", className );
		return embeddedFieldValue;
	}

	public static class GenerationResult {

		private List<Object> preparedStatementParams;
		private String query;

		public GenerationResult(List<Object> preparedStatementParams, String query) {
			this.preparedStatementParams = preparedStatementParams;
			this.query = query;
		}

		public List<Object> getPreparedStatementParams() {
			return preparedStatementParams;
		}

		public String getQuery() {
			return query;
		}

	}

	protected class QueryResult {

		private List<Object> preparedStatementParams = new LinkedList<>();
		private JSONObject json = new JSONObject();

		public List<Object> getPreparedStatementParams() {
			return preparedStatementParams;
		}

		public void setPreparedStatementParams(List<Object> preparedStatementParams) {
			this.preparedStatementParams = preparedStatementParams;
		}

		public JSONObject getJson() {
			return json;
		}

		public void setJson(JSONObject json) {
			this.json = json;
		}

	}

}
