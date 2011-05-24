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

import com.sun.jersey.api.NotFoundException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.opentsdb.core.TSDB;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.uid.NoSuchUniqueName;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.joda.time.LocalDateTime;
import org.odata4j.core.ODataConstants;
import org.odata4j.core.OEntities;

import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OLink;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmAssociation;
import org.odata4j.edm.EdmAssociationSet;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmType;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.EntitiesResponse;
import org.odata4j.producer.EntityResponse;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.Responses;

/**
 *
 * @author htrippaers
 */
public class OpenTSDBProducer implements ODataProducer {
    
    private final static Logger log = 
            Logger.getLogger(OpenTSDBProducer.class.getName());
    
    private final TSDB tsdb;
    private final EdmDataServices metadata; 
    
    public OpenTSDBProducer(final TSDB tsdb) {
        super();

        // Create the TSDB instance
        this.tsdb = tsdb;
        metadata = InitializeMetaData();
    }

    @Override
    public EdmDataServices getMetadata() {
        log.logp(Level.INFO, this.getClass().getName(), "getMetadata", null);
        return metadata;
    }

    @Override
    public EntitiesResponse getEntities(String entitySetName, QueryInfo queryInfo) {
        log.logp(Level.INFO, this.getClass().getName(), "getEntities", "entitySetName = {0}, queryInfo = {1}", 
                new Object[] { entitySetName, queryInfo.toString() });
        
        if ("Metrics".equals(entitySetName)) {
            return getMetrics(entitySetName, queryInfo);
        }
        else if ("Timeseries".equals(entitySetName)) {
            return getTimeseries(entitySetName, queryInfo);
        }
        throw new NotFoundException("No entity named :" + entitySetName);
    }

    @Override
    public EntityResponse getEntity(String entitySetName, OEntityKey entityKey) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public BaseResponse getNavProperty(String entitySetName, OEntityKey entityKey, String navProp, QueryInfo queryInfo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityResponse createEntity(String entitySetName, OEntity entity) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityResponse createEntity(String entitySetName, OEntityKey entityKey, String navProp, OEntity entity) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteEntity(String entitySetName, OEntityKey entityKey) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void mergeEntity(String entitySetName, OEntity entity) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateEntity(String entitySetName, OEntity entity) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
 
    private EdmDataServices InitializeMetaData() {
        List<EdmSchema> schemas = new ArrayList<EdmSchema>();
        List<EdmEntityContainer> containers = new ArrayList<EdmEntityContainer>();
        List<EdmEntitySet> entitySets = new ArrayList<EdmEntitySet>();
        List<EdmEntityType> entityTypes = new ArrayList<EdmEntityType>();
        List<EdmAssociation> associations = new ArrayList<EdmAssociation>();
        List<EdmAssociationSet> associationSets = new ArrayList<EdmAssociationSet>();
        
        EdmEntityContainer container = new EdmEntityContainer("Container", true, null, entitySets, associationSets, null);
        containers.add(container);

        EdmSchema schema = new EdmSchema("OpenTSDB", null, entityTypes, null, associations, containers);
        schemas.add(schema);
        
        List<String> keys = new ArrayList<String>();
        keys.add("Name");
        List<EdmProperty> properties = new ArrayList<EdmProperty>();
        properties.add(new EdmProperty("Name", EdmType.STRING, false));
        EdmEntityType et = new EdmEntityType("OpenTSDB", "Alias", "Metrics", Boolean.FALSE, keys, properties, null);
        EdmEntitySet es = new EdmEntitySet("Metrics",et);
        entityTypes.add(et);
        entitySets.add(es);

        /* Workaround till funcions are supported */
        List<String> tskeys = new ArrayList<String>();
        keys.add("Timestamp");
        List<EdmProperty> tsproperties = new ArrayList<EdmProperty>();
        tsproperties.add(new EdmProperty("Timestamp", EdmType.DATETIME, false));
        tsproperties.add(new EdmProperty("Value", EdmType.DOUBLE, false));
        EdmEntityType tset = new EdmEntityType("OpenTSDB", "Alias", "Timeseries", Boolean.FALSE, keys, tsproperties, null);
        EdmEntitySet tses = new EdmEntitySet("Timeseries",et);
        entityTypes.add(tset);
        entitySets.add(tses);       
        
        return new EdmDataServices(ODataConstants.DATA_SERVICE_VERSION,schemas);
    }
    
    private EntitiesResponse getMetrics(String entitySetName, QueryInfo queryInfo) {
        EdmEntitySet entitySet = metadata.getEdmEntitySet(entitySetName);
        List<OEntity> items = new ArrayList<OEntity>();
        OEntityKey entityKey = OEntityKey.create("Name");
        

        List<String> names  = tsdb.getMetrics();
        ArrayList<ArrayList<KeyValue>> rows;
        
        try {
            for (String name : names) {
                    List<OProperty<?>> properties = new ArrayList<OProperty<?>>();
                    properties.add(OProperties.string("Name", new String(name)));
                    List<OLink> links = new ArrayList<OLink>();
                    items.add(OEntities.create(entitySet, entityKey, properties, links));
            }
        } catch (Exception ex) {
            /* TODO: some kind of error handing */
            Logger.getLogger(OpenTSDBProducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return Responses.entities(items, entitySet, items.size(), null);
    }
    
    private EntitiesResponse getTimeseries(String entitySetName, QueryInfo queryInfo) {
        EdmEntitySet entitySet = metadata.getEdmEntitySet(entitySetName);
        OEntityKey entityKey = OEntityKey.create("Timestamp");
        List<OEntity> items = new ArrayList<OEntity>();
        
        /* custom properties define the start and end of the timeseries */
        String seriesName = queryInfo.customOptions.containsKey("series") ?
                queryInfo.customOptions.get("series") : null;
        String startTimestamp = queryInfo.customOptions.containsKey("start") ?
                queryInfo.customOptions.get("start") : null;
        String stopTimestamp = queryInfo.customOptions.containsKey("stop") ?
                queryInfo.customOptions.get("stop") : null;
    

        if (seriesName != null) {
            try {
                Query query = tsdb.newQuery();
                Map<String, String> tags = new HashMap<String, String>();
                query.setTimeSeries(seriesName, tags, Aggregators.SUM, false);
                query.setStartTime(System.currentTimeMillis() / 1000 - 86400 * 30);
                query.setEndTime(System.currentTimeMillis() / 1000);
                DataPoints[] result = query.run();
                for (DataPoints dps : result) {
                    SeekableView data = dps.iterator();
                    while (data.hasNext()) {
                        DataPoint dp = data.next();
                        items.add(DataPointToOEntity(dp, entitySet, entityKey));
                    }
                }
            } catch (NoSuchUniqueName ex) {
                log.log(Level.SEVERE, null, ex);
            }
        }

    
        return Responses.entities(items, entitySet, items.size(), null);
    }
    
    private OEntity DataPointToOEntity(DataPoint dp, EdmEntitySet entitySet, OEntityKey entityKey) {
        List<OProperty<?>> properties = new ArrayList<OProperty<?>>();
        List<OLink> links = new ArrayList<OLink>();
        
        LocalDateTime ldt = new LocalDateTime(dp.timestamp() * 1000);
        properties.add(OProperties.datetime("Timestamp", ldt));
        if (dp.isInteger()) {
            Long value = dp.longValue();
            properties.add(OProperties.double_("Value", value.doubleValue()));
        }
        else {
            properties.add(OProperties.double_("Value", dp.doubleValue()));
        }
        return OEntities.create(entitySet, entityKey, properties, links);
        
    }
}

