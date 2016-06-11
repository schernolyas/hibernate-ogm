/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.query.impl;

import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.engine.query.spi.ParameterParser;
import org.hibernate.ogm.dialect.query.spi.RecognizerBasedParameterMetadataBuilder;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.RecoveringParseRunner;

/**
 * The class is builder of parameter metadata
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBParameterMetadataBuilder extends RecognizerBasedParameterMetadataBuilder {

	private static Log LOG = LoggerFactory.getLogger();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void parseQueryParameters(String nativeQuery, ParameterParser.Recognizer journaler) {
		OrientDBQueryParser parser = Parboiled.createParser( OrientDBQueryParser.class, journaler );
		new RecoveringParseRunner<ParameterParser.Recognizer>( parser.Query() ).run( nativeQuery );
	}

}