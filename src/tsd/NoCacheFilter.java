package net.opentsdb.tsd;

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

public final class NoCacheFilter implements ContainerResponseFilter {
	@Override
	public ContainerResponse filter(ContainerRequest request,
			ContainerResponse response) {
		MultivaluedMap<String, Object> map = response.getHttpHeaders();
		map.add("Cache-Control", "max-age=1");
		
		return response;
	}
}