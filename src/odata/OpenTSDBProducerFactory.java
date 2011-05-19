/**
 *  OData provider for OpenTSDB
 *  Copyright (C) 2011  Schuberg Philis
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

//package com.schubergphilis.opentsdb.odata;
package net.opentsdb.odata;

import java.util.Properties;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.ODataProducerFactory;

/**
 *
 * @author htrippaers
 */
public class OpenTSDBProducerFactory implements ODataProducerFactory {
    
    @Override
    public ODataProducer create(Properties arg0) {
        OpenTSDBProducer producer = new OpenTSDBProducer();
         
         return producer;
         
    }    
}

