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

package net.opentsdb.odata;

import java.io.IOException;

import java.net.URI;

import java.util.Map;

import javax.servlet.Servlet;

import com.sun.grizzly.http.SelectorThread;
import com.sun.grizzly.http.servlet.ServletAdapter;
import com.sun.grizzly.standalone.StaticStreamAlgorithm;

import com.sun.jersey.api.container.ContainerException;
import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 *
 * @author htrippaers
 */
public class ODataGrizzlyFactory {
    public static SelectorThread create (final String baseUri, final Map<String, String> initParams) 
            throws IOException 
    {
        if (baseUri.isEmpty()) {
                throw new IllegalArgumentException("Empty URL");
        }

	URI uri = URI.create(baseUri);
        ServletAdapter adapter = new ServletAdapter();
        for (Map.Entry<String, String> e : initParams.entrySet()) {
            adapter.addInitParameter(e.getKey(), e.getValue());
        }
     
        adapter.setServletInstance(getInstance(ServletContainer.class));
	String path = uri.getPath();
        adapter.setContextPath(path.endsWith("/") ? path.substring(0, path.length() - 1): path);
        
        final SelectorThread selectorThread = new SelectorThread();
        selectorThread.setAlgorithmClassName(StaticStreamAlgorithm.class.getName());
        selectorThread.setPort(uri.getPort() != -1 ? uri.getPort() : 80);
        selectorThread.setAdapter(adapter);
        selectorThread.setUseChunking(false);
        try {
            selectorThread.listen();
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        
        return selectorThread;   
    }
    
    static private Servlet getInstance(Class<? extends Servlet> servlet) {
        try {
            return servlet.newInstance();
        } catch (Exception e) {
            throw new ContainerException(e);
        }
    }
}

