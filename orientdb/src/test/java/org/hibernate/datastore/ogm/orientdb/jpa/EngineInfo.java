/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.jpa;

import javax.persistence.Embeddable;
import javax.persistence.Embedded;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
@Embeddable
public class EngineInfo {

	private String title;
	private int power;
	private short cylinders;
	private long price;
	@Embedded
	private Producer producer;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public int getPower() {
		return power;
	}

	public void setPower(int power) {
		this.power = power;
	}

	public short getCylinders() {
		return cylinders;
	}

	public void setCylinders(short cylinders) {
		this.cylinders = cylinders;
	}

	public long getPrice() {
		return price;
	}

	public void setPrice(long price) {
		this.price = price;
	}

	public Producer getProducer() {
		return producer;
	}

	public void setProducer(Producer producer) {
		this.producer = producer;
	}

}