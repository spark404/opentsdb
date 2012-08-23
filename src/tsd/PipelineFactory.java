// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.api.core.ApplicationAdapter;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.server.impl.container.netty.NettyHandlerContainer;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import net.opentsdb.core.TSDB;
import net.opentsdb.odata.OpenTSDBProducer;
import net.opentsdb.odata.OpenTSDBProducerFactory;
import org.odata4j.jersey.producer.resources.ODataProducerProvider;
import org.odata4j.jersey.producer.resources.ODataApplication;

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 * This class is supposed to be a singleton.
 */
public final class PipelineFactory implements ChannelPipelineFactory {

  // Those are entirely stateless and thus a single instance is needed.
  private static final ChannelBuffer[] DELIMITERS = Delimiters.lineDelimiter();
  private static final StringEncoder ENCODER = new StringEncoder();
  private static final WordSplitter DECODER = new WordSplitter();

  // Those are sharable but maintain some state, so a single instance per
  // PipelineFactory is needed.
  private final ConnectionManager connmgr = new ConnectionManager();
  private final DetectHttpOrRpc HTTP_OR_RPC = new DetectHttpOrRpc();

  /** Stateless handler for RPCs. */
  private final RpcHandler rpchandler;
  /** Handler for OData */
  private final NettyHandlerContainer odatahandler;

  /**
   * Constructor.
   * @param tsdb The TSDB to use.
   */
  public PipelineFactory(final TSDB tsdb, final URI baseUri) {
    this.rpchandler = new RpcHandler(tsdb);
    
    /**
     * Set properties 
     */
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(NettyHandlerContainer.PROPERTY_BASE_URI, baseUri.toString() + "/odata.svc/");
    props.put(ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, "net.opentsdb.tsd.NoCacheFilter");
    
    ODataProducerProvider.setInstance(new OpenTSDBProducer(tsdb));
    ODataApplication app = new ODataApplication();
    
    /**
     * Create the resource configuration
     */
    ResourceConfig rc = new ApplicationAdapter(app);
    rc.setPropertiesAndFeatures(props);
    
    this.odatahandler = ContainerFactory.createContainer(NettyHandlerContainer.class, rc);
    
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
   final ChannelPipeline pipeline = pipeline();

    pipeline.addLast("connmgr", connmgr);
    pipeline.addLast("detect", HTTP_OR_RPC);
    return pipeline;
  }

  /**
   * Dynamically changes the {@link ChannelPipeline} based on the request.
   * If a request uses HTTP, then this changes the pipeline to process HTTP.
   * Otherwise, the pipeline is changed to processes an RPC.
   */
  final class DetectHttpOrRpc extends FrameDecoder {

    @Override
    protected Object decode(final ChannelHandlerContext ctx,
                            final Channel chan,
                            final ChannelBuffer buffer) throws Exception {
      if (buffer.readableBytes() < 1) {  // Yes sometimes we can be called
        return null;                     // with an empty buffer...
      }

      final int firstbyte = buffer.getUnsignedByte(buffer.readerIndex());
      final ChannelPipeline pipeline = ctx.getPipeline();
      // None of the commands in the RPC protocol start with a capital ASCII
      // letter for the time being, and all HTTP commands do (GET, POST, etc.)
      // so use this as a cheap way to differentiate the two.
      if ('A' <= firstbyte && firstbyte <= 'Z') {
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());
      } else {
        pipeline.addLast("framer",
                         new DelimiterBasedFrameDecoder(1024, DELIMITERS));
        pipeline.addLast("encoder", ENCODER);
        pipeline.addLast("decoder", DECODER);
      }
      pipeline.remove(this);
      pipeline.addLast("handler", rpchandler);
      pipeline.addLast("odatahandler", odatahandler);

      // Forward the buffer to the next handler.
      return buffer.readBytes(buffer.readableBytes());
    }

  }

}
