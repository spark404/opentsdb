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
import java.lang.Thread.State;

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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.WebApplicationException;
import net.opentsdb.core.Aggregator;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.joda.time.DateTime;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.odata4j.core.ODataConstants;
import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityId;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OFunctionParameter;
import org.odata4j.core.OLink;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmAssociation;
import org.odata4j.edm.EdmAssociationSet;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmFunctionImport;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.CountResponse;
import org.odata4j.producer.EntitiesResponse;
import org.odata4j.producer.EntityIdResponse;
import org.odata4j.producer.EntityQueryInfo;
import org.odata4j.producer.EntityResponse;
import org.odata4j.producer.InlineCount;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.Responses;
import org.odata4j.producer.edm.MetadataProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author htrippaers
 */
public class OpenTSDBProducer implements ODataProducer {
    
    private static final Logger LOG = LoggerFactory.getLogger(OpenTSDBProducer.class);
    
    private final TSDB tsdb;
    private EdmDataServices metadata;
    private final Cache queryCache;
    
    public OpenTSDBProducer(final TSDB tsdb) {
        super();

        // Create the TSDB instance
        this.tsdb = tsdb;
        
        // Setup ehcache
        CacheManager cacheManager = CacheManager.create();
        queryCache = new Cache("queryCache", 10, false, false, 600, 2);
        cacheManager.addCache(queryCache);
    }

	@Override
	public BaseResponse callFunction(EdmFunctionImport arg0,
			Map<String, OFunctionParameter> arg1, QueryInfo arg2) {
		LOG.debug("Entering callFunction (EdmFunctionImport, Map, QueryInfo)");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void close() {
		LOG.debug("Entering close");
		CacheManager.getInstance().shutdown();
	}

	@Override
	public EntityResponse createEntity(String arg0, OEntity arg1) {
		LOG.debug("Entering createEntity (String, OEntity");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public EntityResponse createEntity(String arg0, OEntityKey arg1,
			String arg2, OEntity arg3) {
		LOG.debug("Entering createEntity (String, OEntityKey, String, OEntity)");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void createLink(OEntityId arg0, String arg1, OEntityId arg2) {
		LOG.debug("Entering createLink");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void deleteEntity(String arg0, OEntityKey arg1) {
		LOG.debug("Entering deleteEntity");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void deleteLink(OEntityId arg0, String arg1, OEntityKey arg2) {
		LOG.debug("Entering deleteLink");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public EntitiesResponse getEntities(String entitySetName, QueryInfo queryInfo) {
		LOG.debug("Entering getEntities (String, QueryInfo)");
		if ("MetricList".equals(entitySetName)) {
			return getMetrics(entitySetName, queryInfo);
		}
		else if ("Timeseries".equals(entitySetName)) {
			return getTimeseries(entitySetName, queryInfo);
		}
		throw new NotFoundException("No entity named :" + entitySetName);
	}

	@Override
	public CountResponse getEntitiesCount(String arg0, QueryInfo arg1) {
		LOG.debug("Entering getEntitiesCount (String, QueryInfo)");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public EntityResponse getEntity(String arg0, OEntityKey arg1,
			EntityQueryInfo arg2) {
		LOG.debug("Entering getEntity (String, OEntityKey, EntityQueryInfo)");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public EntityIdResponse getLinks(OEntityId arg0, String arg1) {
		LOG.debug("Entering getLinks (OEntityId, String)");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public EdmDataServices getMetadata() {
		LOG.debug("Entering getMetadata");
		if (metadata == null) {
			metadata = new OpenTSDBEdmGenerator().generateEdm(null).build();
		}
		return metadata;
	}

	@Override
	public MetadataProducer getMetadataProducer() {
		LOG.debug("Entering getMetadataProducer");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public BaseResponse getNavProperty(String arg0, OEntityKey arg1,
			String arg2, QueryInfo arg3) {
		LOG.debug("Entering getNavProperty");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public CountResponse getNavPropertyCount(String arg0, OEntityKey arg1,
			String arg2, QueryInfo arg3) {
		LOG.debug("Entering getNavPropertyCount");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void mergeEntity(String arg0, OEntity arg1) {
		LOG.debug("Entering mergeEntity");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void updateEntity(String arg0, OEntity arg1) {
		LOG.debug("Entering updateEntity");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void updateLink(OEntityId arg0, String arg1, OEntityKey arg2,
			OEntityId arg3) {
		LOG.debug("Entering updateLink");
		throw new UnsupportedOperationException("Not supported yet.");
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
	
	private EntitiesResponse getTimeseries(String entitySetName,
			QueryInfo queryInfo) {
		List<OEntity> items = new ArrayList<OEntity>();
		Map<String, String> tags = new HashMap<String, String>(
				queryInfo.customOptions);

		/** Get some timing figures */
		long now = System.currentTimeMillis();

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

		if (!(queryInfo.customOptions.containsKey("series") && queryInfo.customOptions
				.containsKey("start"))) {
			throw new NotFoundException(
					"Invalid parameters: series and start need to be present");
		}

		String seriesName = queryInfo.customOptions.get("series");

		try {
			DataPoints[] resultSets;
			String cacheKey = createCacheHash(queryInfo);
			if (! queryCache.isKeyInCache(cacheKey)) { 
				LOG.debug("Cache miss");
				Query query = tsdb.newQuery();

				Aggregator agg = Aggregators.get(queryInfo.customOptions
						.containsKey("aggregator") ? queryInfo.customOptions
						.get("aggregator") : "sum");
				boolean rate = queryInfo.customOptions.containsKey("rate") ? queryInfo.customOptions
						.get("rate").equalsIgnoreCase("true") : false;
				query.setTimeSeries(seriesName, tags, agg, rate);

				query.setStartTime(parseDateTimeParameter(queryInfo.customOptions
						.get("start")));

				if (queryInfo.customOptions.containsKey("stop"))
					query.setEndTime(parseDateTimeParameter(queryInfo.customOptions
							.get("stop")));

				if (queryInfo.customOptions.containsKey("downsample")) {
					String[] dsSettings = queryInfo.customOptions.get(
							"downsample").split(":", 2);
					Aggregator dsAgg = Aggregators.get(dsSettings[0]);
					int dsInt = Integer.parseInt(dsSettings[1]);
					query.downsample(dsInt, dsAgg);
				}

				resultSets = query.run();
				queryCache.put(new Element(cacheKey, resultSets));
			} else {
				LOG.debug("Cache hit");
				resultSets = (DataPoints[]) queryCache.get(cacheKey).getObjectValue();
			}

			LOG.info("Returned " + resultSets.length + " DataPoint arrays in "
					+ (System.currentTimeMillis() - now) + "ms");
			now = System.currentTimeMillis();

			/**
			 * Build a list of all common tags across the series and update the
			 * entity set
			 */
			Set<String> commonTags = new HashSet<String>();
			for (DataPoints dps : resultSets) {
				commonTags.addAll(dps.getTags().keySet());
			}

			EdmEntitySet entitySet = tagsToEdmEntitySet(commonTags);
			OEntityKey entityKey = OEntityKey.create("Timestamp");

			if (LOG.isDebugEnabled()) {
				StringBuilder str = new StringBuilder("Common tags :");
				for (String tag : commonTags) {
					str.append(" ");
					str.append(tag);
				}
				LOG.debug(str.toString());
			}

			int itemCount = 0;
			for (DataPoints dps : resultSets) {
				itemCount += dps.size();
				SeekableView data = dps.iterator();
				Map<String, String> dpTags = dps.getTags();
				while (data.hasNext()) {
					if ((queryInfo.top != null)
							&& (queryInfo.top <= items.size())) {
						break;
					}
					DataPoint dp = data.next();
					items.add(DataPointToOEntity(dp, entitySet, entityKey,
							seriesName, dpTags));
				}
			}
			LOG.info("Prepared OData reponse ( " + items.size() + " of "
					+ itemCount + " items) in "
					+ (System.currentTimeMillis() - now) + "ms");

			return Responses.entities(items, entitySet,
					(queryInfo.inlineCount == InlineCount.ALLPAGES) ? itemCount
							: null, null);

		} catch (NoSuchUniqueName ex) {
			// rethrow as WebApplicationException
			throw new NotFoundException("No timeseries named :" + seriesName);
		} catch (IllegalArgumentException ex) {
			// rethrow as WebApplicationException
			throw new WebApplicationException(ex);
		}
	}

	private EdmEntitySet tagsToEdmEntitySet(Set<String> tags) {
		/**
		 * The type definition for TimeSeries
		 */
		List<EdmProperty.Builder> properties = new ArrayList<EdmProperty.Builder>();
		properties.add(EdmProperty.newBuilder("Name").setName("Name")
				.setType(EdmSimpleType.STRING));
		properties.add(EdmProperty.newBuilder("Timestamp").setName("Timestamp")
				.setType(EdmSimpleType.DATETIME));
		properties.add(EdmProperty.newBuilder("Value").setName("Value")
				.setType(EdmSimpleType.DOUBLE));
		for (String tag : tags) {
			properties.add(EdmProperty.newBuilder(tag).setName(tag)
					.setType(EdmSimpleType.STRING));
		}
		EdmEntityType.Builder timeSeriesType = EdmEntityType.newBuilder()
				.setName("TimeSeries").setNamespace("OpenTSDB")
				.addKeys(new String[] { "Name", "Timestamp" })
				.addProperties(properties);
		EdmEntitySet.Builder timeSeriesSet = EdmEntitySet.newBuilder()
				.setName("TimeSeries").setEntityType(timeSeriesType);

		return timeSeriesSet.build();
	}

	private OEntity DataPointToOEntity(DataPoint dp, EdmEntitySet entitySet,
			OEntityKey entityKey, String metric, Map<String, String> tags) {
		List<OProperty<?>> properties = new ArrayList<OProperty<?>>();
		List<OLink> links = new ArrayList<OLink>();

		properties.add(OProperties.string("Name", metric));
		LocalDateTime ldt = new LocalDateTime(dp.timestamp() * 1000);
		properties.add(OProperties.datetime("Timestamp", ldt));
		if (dp.isInteger()) {
			Long value = dp.longValue();
			properties.add(OProperties.double_("Value", value.doubleValue()));
		} else {
			properties.add(OProperties.double_("Value", dp.doubleValue()));
		}
		for (String tag : tags.keySet()) {
			properties.add(OProperties.string(tag,
					tags.containsKey(tag) ? tags.get(tag) : null));
		}
		return OEntities.create(entitySet, entityKey, properties, links);

	}

	/**
	 * Convert a string to a unix timestamp
	 * 
	 * @param dateTimeParameter
	 *            formatted as "2011/05/26-10:59:00"
	 * @return seconds since epoch (aka unix timestamp)
	 */
	private long parseDateTimeParameter(String dateTimeParameter) {
		assert (dateTimeParameter != null);
		DateTimeFormatter fmt = DateTimeFormat
				.forPattern("yyyy/MM/dd-HH:mm:ss");
		DateTime dt = fmt.parseDateTime(dateTimeParameter);
		return dt.getMillis() / 1000; /* only interested in seconds */
	}
	
    /**
     * The function generates the unique id for the cache entry.
     * @param query
     * @return 
     */
    public static String createCacheHash(QueryInfo query) {
        StringBuilder hashinput = new StringBuilder();
        
        SortedMap<String,String> s = new TreeMap<String,String>(query.customOptions);
        for (Map.Entry<String, String> entry : s.entrySet()) {
            hashinput.append(entry.getKey());
            hashinput.append(entry.getValue());
        }
        
        return hashinput.toString();
    }
	

}
