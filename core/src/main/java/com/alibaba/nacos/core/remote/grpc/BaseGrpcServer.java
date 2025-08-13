/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.remote.grpc;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.api.remote.response.ErrorResponse;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.grpc.GrpcUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.monitor.MetricsMonitor;
import com.alibaba.nacos.core.remote.BaseRpcServer;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.remote.RequestHandlerRegistry;
import com.alibaba.nacos.core.remote.grpc.negotiator.NacosGrpcProtocolNegotiator;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.InetUtils;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerTransportFilter;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Grpc implementation as a rpc server.
 *
 * @author liuzunfei
 * @version $Id: BaseGrpcServer.java, v 0.1 2020年07月13日 3:42 PM liuzunfei Exp $
 */
public abstract class BaseGrpcServer extends BaseRpcServer {
    
    /**
     * The ProtocolNegotiator instance used for communication.
     */
    protected NacosGrpcProtocolNegotiator protocolNegotiator;
    
    private Server server;
    
    @Autowired
    private GrpcRequestAcceptor grpcCommonRequestAcceptor;
    
    @Autowired
    private GrpcBiStreamRequestAcceptor grpcBiStreamRequestAcceptor;
    
    @Autowired
    private ConnectionManager connectionManager;
    
    @Autowired
    private RequestHandlerRegistry requestHandlerRegistry;
    
    @Override
    public ConnectionType getConnectionType() {
        return ConnectionType.GRPC;
    }
    
    /**
     * 启动 gRPC 服务器
     * 这是 Nacos gRPC 服务器启动的核心方法，负责完成服务器的完整初始化和启动流程
     *
     * 主要执行步骤：
     * 1. 创建服务注册表并注册 gRPC 服务（一元调用和双向流服务）
     * 2. 配置服务器监听地址和端口
     * 3. 创建并配置 NettyServerBuilder（线程池、协议协商、过滤器等）
     * 4. 设置服务器各种参数（消息大小、Keep-Alive、压缩等）
     * 5. 构建并启动服务器实例
     *
     * @throws Exception 启动过程中的任何异常
     */
    @Override
    public void startServer() throws Exception {
        // ========== 第一阶段：服务注册阶段 ==========
        // 创建可变的服务处理器注册表，用于动态管理 gRPC 服务
        // MutableHandlerRegistry 允许在运行时添加、移除和修改服务定义
        final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();
        
        // 注册 gRPC 服务到服务器
        // 这里会注册两个核心服务：
        // 1. 一元调用服务 (REQUEST_SERVICE_NAME/REQUEST_METHOD_NAME) - 用于同步请求响应
        // 2. 双向流服务 (REQUEST_BI_STREAM_SERVICE_NAME/REQUEST_BI_STREAM_METHOD_NAME) - 用于实时通信
        // 同时应用服务器拦截器进行统一的请求处理（认证、日志、监控等）
        addServices(handlerRegistry, getSeverInterceptors().toArray(new ServerInterceptor[0]));
        
        // ========== 第二阶段：网络配置阶段 ==========
        // 获取 gRPC 服务器监听的 IP 地址
        // 如果配置了特定的监听 IP，则使用配置的 IP；否则监听所有网络接口
        String grpcListenIp = InetUtils.getGrpcListenIp();
        InetSocketAddress inetSocketAddress = StringUtils.isNotBlank(grpcListenIp)
                // 绑定到指定 IP 和端口
                ? new InetSocketAddress(grpcListenIp, getServicePort())
                // 绑定到所有 IP 的指定端口
                : new InetSocketAddress(getServicePort());
        
        // ========== 第三阶段：服务器构建阶段 ==========
        // 创建基于 Netty 的 gRPC 服务器构建器
        // 配置监听地址和自定义的 RPC 执行器（线程池）
        NettyServerBuilder builder = NettyServerBuilder.forAddress(inetSocketAddress).executor(getRpcExecutor());
        
        // ========== 第四阶段：协议协商配置阶段 ==========
        // 尝试创建协议协商器（如 TLS、代理协议等）
        // 协议协商器用于处理连接建立时的协议选择和安全配置
        Optional<InternalProtocolNegotiator.ProtocolNegotiator> negotiator = newProtocolNegotiator();
        if (negotiator.isPresent()) {
            InternalProtocolNegotiator.ProtocolNegotiator actual = negotiator.get();
            Loggers.REMOTE.info("Add protocol negotiator {}", actual.getClass().getCanonicalName());
            // 将协议协商器应用到服务器构建器
            builder.protocolNegotiator(actual);
        }
        
        // ========== 第五阶段：传输过滤器配置阶段 ==========
        // 添加服务器传输过滤器，用于在传输层进行连接管理和处理
        // 例如：AddressTransportFilter 用于管理客户端连接信息
        for (ServerTransportFilter each : getServerTransportFilters()) {
            builder.addTransportFilter(each);
        }
        
        // ========== 第六阶段：服务器参数配置和构建阶段 ==========
        server = builder
                // 设置最大入站消息大小，防止过大的消息导致内存问题
                .maxInboundMessageSize(getMaxInboundMessageSize())
                // 设置回退处理器注册表，当找不到对应服务时使用
                .fallbackHandlerRegistry(handlerRegistry)
                // 配置压缩器注册表，支持消息压缩以减少网络传输
                .compressorRegistry(CompressorRegistry.getDefaultInstance())
                // 配置解压缩器注册表，支持消息解压缩
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                // 配置 Keep-Alive 时间，定期发送心跳包保持连接活跃
                .keepAliveTime(getKeepAliveTime(), TimeUnit.MILLISECONDS)
                // 配置 Keep-Alive 超时时间，超时后关闭连接
                .keepAliveTimeout(getKeepAliveTimeout(), TimeUnit.MILLISECONDS)
                // 配置允许客户端发送 Keep-Alive 的最小间隔时间，防止过于频繁的心跳
                .permitKeepAliveTime(getPermitKeepAliveTime(), TimeUnit.MILLISECONDS)
                // 构建服务器实例
                .build();
        
        // ========== 第七阶段：服务器启动阶段 ==========
        // 启动 gRPC 服务器，开始监听客户端连接
        // 此时服务器已准备好接收和处理客户端请求
        server.start();
        
        // 服务器启动完成后，客户端可以通过以下方式与服务器通信：
        // 1. 一元调用：发送单个请求，接收单个响应（如配置查询、服务注册）
        // 2. 双向流：建立持久连接，双向发送消息（如配置推送、实时通知）
    }
    
    @Override
    public void reloadProtocolContext() {
        reloadProtocolNegotiator();
    }
    
    /**
     * Build new one protocol negotiator.
     *
     * <p>Such as support tls, proxy protocol and so on</p>
     *
     * @return ProtocolNegotiator
     */
    protected Optional<InternalProtocolNegotiator.ProtocolNegotiator> newProtocolNegotiator() {
        return Optional.empty();
    }
    
    /**
     * reload protocol negotiator If necessary.
     */
    public void reloadProtocolNegotiator() {
        if (protocolNegotiator != null) {
            try {
                protocolNegotiator.reloadNegotiator();
            } catch (Throwable throwable) {
                Loggers.REMOTE.info("Nacos {} Rpc server reload negotiator fail at port {}.",
                        this.getClass().getSimpleName(), getServicePort());
                throw throwable;
            }
        }
    }
    
    protected long getPermitKeepAliveTime() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_PERMIT_KEEP_ALIVE_TIME;
    }
    
    protected long getKeepAliveTime() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_KEEP_ALIVE_TIME;
    }
    
    protected long getKeepAliveTimeout() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_KEEP_ALIVE_TIMEOUT;
    }
    
    protected int getMaxInboundMessageSize() {
        Integer property = EnvUtil.getProperty(GrpcServerConstants.GrpcConfig.MAX_INBOUND_MSG_SIZE_PROPERTY,
                Integer.class);
        if (property != null) {
            return property;
        }
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE;
    }
    
    protected List<ServerInterceptor> getSeverInterceptors() {
        List<ServerInterceptor> result = new LinkedList<>();
        result.add(new GrpcConnectionInterceptor());
        return result;
    }
    
    protected List<ServerTransportFilter> getServerTransportFilters() {
        return Collections.singletonList(new AddressTransportFilter(connectionManager));
    }
    
    /**
     * get source for the request.
     *
     * @return
     */
    protected abstract String getSource();
    
    private boolean invokeSourceAllowCheck(Payload grpcRequest) {
        return requestHandlerRegistry.checkSourceInvokeAllowed(grpcRequest.getMetadata().getType(), getSource());
    }
    
    protected void handleCommonRequest(Payload grpcRequest, StreamObserver<Payload> responseObserver) {
        if (!invokeSourceAllowCheck(grpcRequest)) {
            Payload payloadResponse = GrpcUtils.convert(ErrorResponse.build(NacosException.BAD_GATEWAY,
                    String.format(" invoke %s from %s is forbidden", grpcRequest.getMetadata().getType(),
                            this.getSource())));
            responseObserver.onNext(payloadResponse);
            
            responseObserver.onCompleted();
            MetricsMonitor.recordGrpcRequestEvent(grpcRequest.getMetadata().getType(), false,
                    NacosException.BAD_GATEWAY, null, null, 0);
        } else {
            grpcCommonRequestAcceptor.request(grpcRequest, responseObserver);
        }
    }
    
    /**
     * 注册 gRPC 服务到服务器
     * 这个方法是 gRPC 服务注册的核心，负责将 Nacos 的 RPC 服务暴露给客户端
     *
     * @param handlerRegistry gRPC 服务注册表，用于管理所有的服务定义
     * @param serverInterceptor 服务器拦截器数组，用于处理请求前后的逻辑（如认证、日志等）
     */
    private void addServices(MutableHandlerRegistry handlerRegistry, ServerInterceptor... serverInterceptor) {
        // ========== 注册一元调用服务 (Unary RPC) ==========
        // 一元调用：客户端发送单个请求，服务器返回单个响应
        // 用于处理配置管理、服务注册等同步操作
        final MethodDescriptor<Payload, Payload> unaryPayloadMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                // 设置为一元调用类型
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(
                        // 生成完整的方法名：RequestService/request
                        MethodDescriptor.generateFullMethodName(GrpcServerConstants.REQUEST_SERVICE_NAME,
                                GrpcServerConstants.REQUEST_METHOD_NAME))
                // 请求序列化器
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
                // 响应序列化器
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
        
        // 创建一元调用的处理器，将请求委托给 handleCommonRequest 方法处理
        final ServerCallHandler<Payload, Payload> payloadHandler = ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> handleCommonRequest(request, responseObserver));
        
        // 构建一元调用的服务定义
        final ServerServiceDefinition serviceDefOfUnaryPayload = ServerServiceDefinition.builder(
                GrpcServerConstants.REQUEST_SERVICE_NAME).addMethod(unaryPayloadMethod, payloadHandler).build();
        // 将服务注册到 handlerRegistry，并应用拦截器
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfUnaryPayload, serverInterceptor));
        
        // ========== 注册双向流服务 (Bidirectional Streaming RPC) ==========
        // 双向流：客户端和服务器可以同时发送多个消息，支持实时通信
        // 用于处理配置推送、服务发现变更通知等需要实时性的操作
        final ServerCallHandler<Payload, Payload> biStreamHandler = ServerCalls.asyncBidiStreamingCall(
                // 将双向流请求委托给 grpcBiStreamRequestAcceptor 处理
                (responseObserver) -> grpcBiStreamRequestAcceptor.requestBiStream(responseObserver));
        
        // 创建双向流的方法描述符
        final MethodDescriptor<Payload, Payload> biStreamMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                // 设置为双向流类型
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                .setFullMethodName(
                        // 生成完整的方法名：BiRequestStreamService/requestBiStream
                        MethodDescriptor.generateFullMethodName(GrpcServerConstants.REQUEST_BI_STREAM_SERVICE_NAME,
                                GrpcServerConstants.REQUEST_BI_STREAM_METHOD_NAME))
                // 请求序列化器
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.newBuilder().build()))
                // 响应序列化器
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
        
        // 构建双向流的服务定义
        final ServerServiceDefinition serviceDefOfBiStream = ServerServiceDefinition.builder(
                GrpcServerConstants.REQUEST_BI_STREAM_SERVICE_NAME).addMethod(biStreamMethod, biStreamHandler).build();
        // 将服务注册到 handlerRegistry，并应用拦截器
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfBiStream, serverInterceptor));
        
        // 注册完成后，gRPC 服务器就能够：
        // 1. 接收客户端的一元调用请求（如配置获取、服务注册）
        // 2. 处理客户端的双向流请求（如配置推送、实时通知）
        // 3. 通过拦截器进行统一的请求处理（认证、日志、监控等）
    }
    
    @Override
    public void shutdownServer() {
        if (server != null) {
            server.shutdownNow();
        }
    }
    
    /**
     * get rpc executor.
     *
     * @return executor.
     */
    public abstract ThreadPoolExecutor getRpcExecutor();
    
}
