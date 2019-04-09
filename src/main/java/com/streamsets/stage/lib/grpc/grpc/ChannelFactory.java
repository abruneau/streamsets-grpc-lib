/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.stage.lib.grpc.grpc;

import com.google.auth.Credentials;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;

import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

/** Knows how to construct grpc channels. */
public class ChannelFactory {
  private final ListeningExecutorService authExecutor;
  private final TlsConfigBean tlsConfig;

  public static ChannelFactory create(TlsConfigBean tlsConfig) {
    ListeningExecutorService authExecutor = listeningDecorator(
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build()));
    return new ChannelFactory(authExecutor, tlsConfig);
  }

  public ChannelFactory(ListeningExecutorService authExecutor, TlsConfigBean tlsConfig) {
    this.authExecutor = authExecutor;
    this.tlsConfig = tlsConfig;
  }

  public Channel createChannel(HostAndPort endpoint) {
    NettyChannelBuilder nettyChannelBuilder = createChannelBuilder(endpoint);
    return nettyChannelBuilder.build();
  }

  public Channel createChannelWithCredentials(HostAndPort endpoint, Credentials credentials) {
    return ClientInterceptors.intercept(
        createChannel(endpoint), new ClientAuthInterceptor(credentials, authExecutor));
  }

  private NettyChannelBuilder createChannelBuilder(HostAndPort endpoint) {
    if (!tlsConfig.tlsEnabled) {
      return NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort())
              .negotiationType(NegotiationType.PLAINTEXT);
    } else {
      SslContext sslContext =  new JdkSslContext(tlsConfig.getSslContext(), true, ClientAuth.REQUIRE);
      return NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort())
              .sslContext(sslContext)
              .negotiationType(NegotiationType.TLS);
    }
  }
}
