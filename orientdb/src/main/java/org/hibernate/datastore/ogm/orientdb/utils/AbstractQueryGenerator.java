/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;

/**
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public abstract class AbstractQueryGenerator {
    

	private static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {

		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat( OrientDBConstant.DATETIME_FORMAT );
		}
	};

	

    protected static ThreadLocal<SimpleDateFormat> getFormatter() {
        return FORMATTER;
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
        
        
}
