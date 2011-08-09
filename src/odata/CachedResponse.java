/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.opentsdb.odata;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import net.opentsdb.core.DataPoints;
import org.odata4j.producer.QueryInfo;

/**
 *
 * @author htrippaers
 */
public class CachedResponse {
    private final String cacheHash;
    private final DataPoints[] cachedData;
    private final long cachedTimestamp;
    
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
    
    public CachedResponse(QueryInfo query, DataPoints[] responseData) {
        this.cachedData = responseData;
        this.cachedTimestamp = System.currentTimeMillis();
        this.cacheHash = createCacheHash(query);
    }
    
    public DataPoints[] getCachedData() {
        return cachedData;
    }
    
    public long getCachedTimestamp() {
        return cachedTimestamp;
    }
    
    public String getCacheHash() {
        return cacheHash;
    }
}
