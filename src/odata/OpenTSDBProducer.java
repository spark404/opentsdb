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

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.uid.NoSuchUniqueName;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import net.opentsdb.core.Aggregator;
import org.joda.time.DateTime;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author htrippaers
 */
public class OpenTSDBProducer implements ODataProducer {
    
    private static final Logger LOG = LoggerFactory.getLogger(TSDB.class);
    
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
        return metadata;
    }

    @Override
    public EntitiesResponse getEntities(String entitySetName, QueryInfo queryInfo) { 
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
        tskeys.add("Timestamp");
        List<EdmProperty> tsproperties = new ArrayList<EdmProperty>();
        tsproperties.add(new EdmProperty("Timestamp", EdmType.DATETIME, false));
        tsproperties.add(new EdmProperty("Value", EdmType.DOUBLE, false));
        EdmEntityType tset = new EdmEntityType("OpenTSDB", "Alias", "Timeseries", Boolean.FALSE, tskeys, tsproperties, null);
        EdmEntitySet tses = new EdmEntitySet("Timeseries",tset);
        entityTypes.add(tset);
        entitySets.add(tses);       
        
        return new EdmDataServices(ODataConstants.DATA_SERVICE_VERSION,schemas);
    }
    
    private EntitiesResponse getMetrics(String entitySetName, QueryInfo queryInfo) {
        EdmEntitySet entitySet = metadata.getEdmEntitySet(entitySetName);
        List<OEntity> items = new ArrayList<OEntity>();
        OEntityKey entityKey = OEntityKey.create("Name");
        

        List<String> names  = tsdb.getMetrics();
        
        try {
            for (String name : names) {
                    List<OProperty<?>> properties = new ArrayList<OProperty<?>>();
                    properties.add(OProperties.string("Name", name));
                    List<OLink> links = new ArrayList<OLink>();
                    items.add(OEntities.create(entitySet, entityKey, properties, links));
            }
        } catch (Exception ex) {
            throw new WebApplicationException(ex);
        }
        
        return Responses.entities(items, entitySet, items.size(), null);
    }
    
    private EntitiesResponse getTimeseries(String entitySetName, QueryInfo queryInfo) {
        List<OEntity> items = new ArrayList<OEntity>();        
        Map<String,String> tags = new HashMap<String,String>(queryInfo.customOptions);
        
        /* remove the known keys, the remainder is assumed to be tags */
        if (tags.containsKey("series"))
            tags.remove("series");
        if (tags.containsKey("start"))
            tags.remove("start");
        if (tags.containsKey("stop"))
            tags.remove("stop");
        if (tags.containsKey("aggregator"))
            tags.remove("aggregator");
        if (tags.containsKey("rate"))
            tags.remove("rate");
        if (tags.containsKey("downsample"))
            tags.remove("downsample");

        if (!(queryInfo.customOptions.containsKey("series") && queryInfo.customOptions.containsKey("start"))) {
            throw new NotFoundException("Invalid parameters: series and start need to be present");
        }
        
        String seriesName = queryInfo.customOptions.get("series");        
        
        try {
            Query query = tsdb.newQuery();

            Aggregator agg = Aggregators.get(queryInfo.customOptions.containsKey("aggregator") ? 
                    queryInfo.customOptions.get("aggregator") : "sum");
            boolean rate = queryInfo.customOptions.containsKey("rate") ? 
                    queryInfo.customOptions.get("rate").equalsIgnoreCase("true"): false;
            query.setTimeSeries(seriesName, tags, agg, rate);
            
            query.setStartTime(parseDateTimeParameter(queryInfo.customOptions.get("start")));
            
            if (queryInfo.customOptions.containsKey("stop"))
                query.setEndTime(parseDateTimeParameter(queryInfo.customOptions.get("stop")));
            
            if (queryInfo.customOptions.containsKey("downsample")) {
                String[] dsSettings = queryInfo.customOptions.get("downsample").split(":", 2);
                Aggregator dsAgg = Aggregators.get(dsSettings[0]);
                int dsInt = Integer.parseInt(dsSettings[1]);
                query.downsample(dsInt, dsAgg);
            }
            
            DataPoints[] resultSets = query.run();
            LOG.debug("Returned " + resultSets.length + " DataPoint arrays");
            /**
             * Build a list of all common tags across the series
             * and update the entity set
             */
            Set<String> commonTags = new HashSet();
            for (DataPoints dps : resultSets) {
                commonTags.addAll(dps.getTags().keySet());
            }
            
            EdmEntitySet entitySet = TagsToEdmEntitySet(commonTags);
            OEntityKey entityKey = OEntityKey.create("Timestamp");
            
            if (LOG.isDebugEnabled()) {
                StringBuilder str = new StringBuilder("Common tags :");
                for (String tag : commonTags) {
                    str.append(" ");
                    str.append(tag);
                }
                LOG.debug(str.toString());
            }

            for (DataPoints dps : resultSets) {
                SeekableView data = dps.iterator();
                Map<String,String> dpTags = dps.getTags();
                while (data.hasNext()) {
                    DataPoint dp = data.next();
                    items.add(DataPointToOEntity(dp, entitySet, entityKey, dpTags));
                }
            }

            return Responses.entities(items, entitySet, items.size(), null);
            
        } catch (NoSuchUniqueName ex) {
            // rethrow as WebApplicationException
            throw new NotFoundException("No timeseries named :" + seriesName);
        } catch (IllegalArgumentException ex) {
            // rethrow as WebApplicationException
            throw new WebApplicationException(ex);
        }
    }
    
    private EdmEntitySet TagsToEdmEntitySet(Set<String> tags) {
            List<String> tskeys = new ArrayList<String>();
            tskeys.add("Timestamp");
            List<EdmProperty> tsproperties = new ArrayList<EdmProperty>();
            tsproperties.add(new EdmProperty("Timestamp", EdmType.DATETIME, false));
            tsproperties.add(new EdmProperty("Value", EdmType.DOUBLE, false));
            for (String tag : tags) {
                tsproperties.add(new EdmProperty(tag, EdmType.STRING, true));
            }
            EdmEntityType tset = new EdmEntityType("OpenTSDB", "Alias", "Timeseries", Boolean.FALSE, tskeys, tsproperties, null);
            return new EdmEntitySet("Timeseries", tset);
    }
    
    private OEntity DataPointToOEntity(DataPoint dp, EdmEntitySet entitySet, OEntityKey entityKey, Map<String,String> tags) {
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
        for (String tag: tags.keySet()) {
            properties.add(OProperties.string(tag, tags.containsKey(tag) ? tags.get(tag) : null));
        }
        return OEntities.create(entitySet, entityKey, properties, links);
        
    }
    
    /**
     * Convert a string to a unix timestamp
     * @param dateTimeParameter formatted as "2011/05/26-10:59:00"
     * @return seconds since epoch (aka unix timestamp)
     */
    private long parseDateTimeParameter(String dateTimeParameter) {
        assert(dateTimeParameter != null);
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy/MM/dd-HH:mm:ss");
        DateTime dt = fmt.parseDateTime(dateTimeParameter);
        return dt.getMillis() / 1000; /* only interested in seconds */
    }
}

