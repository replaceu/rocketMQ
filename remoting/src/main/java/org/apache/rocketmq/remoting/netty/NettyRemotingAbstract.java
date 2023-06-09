/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.AttributesBuilder;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_IS_LONG_POLLING;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_ONEWAY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_PROCESS_REQUEST_FAILED;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_WRITE_CHANNEL_FAILED;

//Netty通信抽象类，定义并封装了服务端和客户端公共方法
public abstract class NettyRemotingAbstract {

	/**
	 * Remoting logger instance.
	 */
	private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

	/**
	 * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
	 * oneway请求的信号量
	 */
	protected final Semaphore semaphoreOneway;

	/**
	 * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
	 * async请求的信号量
	 */
	protected final Semaphore semaphoreAsync;

	/**
	 * This map caches all on-going requests.
	 * 缓存所有进行中的请求(因为请求是并行的，要对对应的请求做出对应响应)
	 */
	protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);

	/**
	 * This container holds all processors per request code, aka, for each incoming request, we may look up the
	 * responding processor in this map to handle the request.
	 *  对请求码进行对应的处理
	 */
	protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<>(64);

	/**
	 * Executor to feed netty events to user defined {@link ChannelEventListener}.
	 */
	protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

	/**
	 * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
	 * 默认请求处理器
	 */
	protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessorPair;

	/**
	 * SSL context via which to create {@link SslHandler}.
	 * SSL上下文
	 */
	protected volatile SslContext sslContext;

	/**
	 * custom rpc hooks
	 */
	protected List<RPCHook> rpcHooks = new ArrayList<>();

	static {
		NettyLogger.initNettyLogger();
	}

	/**
	 * Constructor, specifying capacity of one-way and asynchronous semaphores.
	 *
	 * @param permitsOneway Number of permits for one-way requests.
	 * @param permitsAsync Number of permits for asynchronous requests.
	 */
	public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
		this.semaphoreOneway = new Semaphore(permitsOneway, true);
		this.semaphoreAsync = new Semaphore(permitsAsync, true);
	}

	/**
	 * Custom channel event listener.
	 *
	 * @return custom channel event listener if defined; null otherwise.
	 */
	public abstract ChannelEventListener getChannelEventListener();

	/**
	 * Put a netty event to the executor.
	 *
	 * @param event Netty event instance.
	 */
	public void putNettyEvent(final NettyEvent event) {
		this.nettyEventExecutor.putNettyEvent(event);
	}

	/**
	 * Entry of incoming command processing.
	 *
	 * <p>
	 * <strong>Note:</strong>
	 * The incoming remoting command may be
	 * <ul>
	 * <li>An inquiry request from a remote peer component;</li>
	 * <li>A response to a previous request issued by this very participant.</li>
	 * </ul>
	 * </p>
	 *
	 * @param ctx Channel handler context.
	 * @param msg incoming remoting command.
	 */
	public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
		if (msg != null) {
			//该请求可能是一个 request, 也可能是自己发出一个请求的 response
			switch (msg.getType()) {
			case REQUEST_COMMAND:
				processRequestCommand(ctx, msg);
				break;
			case RESPONSE_COMMAND:
				processResponseCommand(ctx, msg);
				break;
			default:
				break;
			}
		}
	}

	protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
		if (rpcHooks.size() > 0) {
			for (RPCHook rpcHook : rpcHooks) {
				rpcHook.doBeforeRequest(addr, request);
			}
		}
	}

	public void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
		if (rpcHooks.size() > 0) {
			for (RPCHook rpcHook : rpcHooks) {
				rpcHook.doAfterResponse(addr, request, response);
			}
		}
	}

	public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response) {
		writeResponse(channel, request, response, null);
	}

	public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response, Consumer<Future<?>> callback) {
		if (response == null) { return; }
		AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder().put(LABEL_IS_LONG_POLLING, request.isSuspended()).put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode())).put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()));
		if (request.isOnewayRPC()) {
			attributesBuilder.put(LABEL_RESULT, RESULT_ONEWAY);
			RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
			return;
		}
		response.setOpaque(request.getOpaque());
		response.markResponseType();
		try {
			channel.writeAndFlush(response).addListener((ChannelFutureListener) future -> {
				if (future.isSuccess()) {
					log.debug("Response[request code: {}, response code: {}, opaque: {}] is written to channel{}", request.getCode(), response.getCode(), response.getOpaque(), channel);
				} else {
					log.error("Failed to write response[request code: {}, response code: {}, opaque: {}] to channel{}", request.getCode(), response.getCode(), response.getOpaque(), channel, future.cause());
				}
				attributesBuilder.put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future));
				RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
				if (callback != null) {
					callback.accept(future);
				}
			});
		} catch (Throwable e) {
			log.error("process request over, but response failed", e);
			log.error(request.toString());
			log.error(response.toString());
			attributesBuilder.put(LABEL_RESULT, RESULT_WRITE_CHANNEL_FAILED);
			RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
		}
	}

	/**
	 * Process incoming request command issued by remote peer.
	 *
	 * @param ctx channel handler context.
	 * @param cmd request command.
	 */
	public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
		//根据请求业务码，获取对应的处理类和线程池
		final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
		final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessorPair : matched;
		final int opaque = cmd.getOpaque();

		if (pair == null) {
			String error = " request type " + cmd.getCode() + " not supported";
			final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
			response.setOpaque(opaque);
			writeResponse(ctx.channel(), cmd, response);
			log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
			return;
		}

		Runnable run = buildProcessRequestHandler(ctx, cmd, pair, opaque);
		//如果拒绝请求为TRUE，是否触发流控
		if (pair.getObject1().rejectRequest()) {
			final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[REJECTREQUEST]system busy, start flow control for a while");
			response.setOpaque(opaque);
			writeResponse(ctx.channel(), cmd, response);
			return;
		}

		try {
			//封装task，使用当前业务的processor绑定的线程池执行
			final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
			//async execute task, current thread return directly
			//交由对应的处理器处理
			pair.getObject2().submit(requestTask);
		} catch (RejectedExecutionException e) {
			if ((System.currentTimeMillis() % 10000) == 0) {
				log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + ", too many requests and system thread pool busy, RejectedExecutionException " + pair.getObject2().toString() + " request code: " + cmd.getCode());
			}

			final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[OVERLOAD]system busy, start flow control for a while");
			response.setOpaque(opaque);
			writeResponse(ctx.channel(), cmd, response);
		} catch (Throwable e) {
			AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder().put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(cmd.getCode())).put(LABEL_RESULT, RESULT_PROCESS_REQUEST_FAILED);
			RemotingMetricsManager.rpcLatency.record(cmd.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
		}
	}

	private Runnable buildProcessRequestHandler(ChannelHandlerContext ctx, RemotingCommand cmd, Pair<NettyRequestProcessor, ExecutorService> pair, int opaque) {
		return () -> {
			Exception exception = null;
			RemotingCommand response;

			try {
				String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
				try {
					//前置处理
					doBeforeRpcHooks(remoteAddr, cmd);
				} catch (AbortProcessException e) {
					throw e;
				} catch (Exception e) {
					exception = e;
				}

				if (exception == null) {
					//核心处理方法
					response = pair.getObject1().processRequest(ctx, cmd);
				} else {
					response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, null);
				}
				try {
					//后置处理
					doAfterRpcHooks(remoteAddr, cmd, response);
				} catch (AbortProcessException e) {
					throw e;
				} catch (Exception e) {
					exception = e;
				}

				if (exception != null) { throw exception; }

				writeResponse(ctx.channel(), cmd, response);
			} catch (AbortProcessException e) {
				response = RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage());
				response.setOpaque(opaque);
				writeResponse(ctx.channel(), cmd, response);
			} catch (Throwable e) {
				log.error("process request exception", e);
				log.error(cmd.toString());

				if (!cmd.isOnewayRPC()) {
					response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, UtilAll.exceptionSimpleDesc(e));
					response.setOpaque(opaque);
					writeResponse(ctx.channel(), cmd, response);
				}
			}
		};
	}

	/**
	 * Process response from remote peer to the previous issued requests.
	 *
	 * @param ctx channel handler context.
	 * @param cmd response command instance.
	 */
	public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
		final int opaque = cmd.getOpaque();
		final ResponseFuture responseFuture = responseTable.get(opaque);
		if (responseFuture != null) {
			responseFuture.setResponseCommand(cmd);

			responseTable.remove(opaque);
			//处理回调方法
			if (responseFuture.getInvokeCallback() != null) {
				executeInvokeCallback(responseFuture);
			} else {
				//如果不是的话，说明这是一个阻塞调用，还需要去进行释放
				responseFuture.putResponse(cmd);
				responseFuture.release();
			}
		} else {
			log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
			log.warn(cmd.toString());
		}
	}

	/**
	 * Execute callback in callback executor. If callback executor is null, run directly in current thread
	 * 在回调执行器中执行回调。如果回调执行器为空，则直接在当前线程中运行
	 */
	private void executeInvokeCallback(final ResponseFuture responseFuture) {
		boolean runInThisThread = false;
		ExecutorService executor = this.getCallbackExecutor();
		if (executor != null && !executor.isShutdown()) {
			try {
				executor.submit(() -> {
					try {
						responseFuture.executeInvokeCallback();
					} catch (Throwable e) {
						log.warn("execute callback in executor exception, and callback throw", e);
					} finally {
						responseFuture.release();
					}
				});
			} catch (Exception e) {
				runInThisThread = true;
				log.warn("execute callback in executor exception, maybe executor busy", e);
			}
		} else {
			runInThisThread = true;
		}

		if (runInThisThread) {
			try {
				responseFuture.executeInvokeCallback();
			} catch (Throwable e) {
				log.warn("executeInvokeCallback Exception", e);
			} finally {
				responseFuture.release();
			}
		}
	}

	/**
	 * Custom RPC hooks.
	 *
	 * @return RPC hooks if specified; null otherwise.
	 */
	public List<RPCHook> getRPCHook() {
		return rpcHooks;
	}

	public void registerRPCHook(RPCHook rpcHook) {
		if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
			rpcHooks.add(rpcHook);
		}
	}

	public void clearRPCHook() {
		rpcHooks.clear();
	}

	/**
	 * This method specifies thread pool to use while invoking callback methods.
	 *
	 * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
	 * netty event-loop thread.
	 * 执行回调方法需要的线程池
	 */
	public abstract ExecutorService getCallbackExecutor();

	/**
	 * <p>
	 * This method is periodically invoked to scan and expire deprecated request.
	 * </p>
	 *
	 * 定期调用此方法来扫描和过期已弃用的请求。
	 */
	public void scanResponseTable() {
		final List<ResponseFuture> rfList = new LinkedList<>();
		Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Integer, ResponseFuture> next = it.next();
			ResponseFuture rep = next.getValue();

			if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
				rep.release();
				it.remove();
				rfList.add(rep);
				log.warn("remove timeout request, " + rep);
			}
		}

		for (ResponseFuture rf : rfList) {
			try {
				executeInvokeCallback(rf);
			} catch (Throwable e) {
				log.warn("scanResponseTable, operationComplete Exception", e);
			}
		}
	}

	public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
		//get the request id
		//相当于requestID，每个请求都会生成一个唯一ID，每次加一
		final int opaque = request.getOpaque();

		try {
			final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
			this.responseTable.put(opaque, responseFuture);
			final SocketAddress addr = channel.remoteAddress();
			//使用Netty的Channel发送请求数据到服务端
			channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
				if (f.isSuccess()) {
					responseFuture.setSendRequestOK(true);
					return;
				}
				// 发送失败，回填responseFuture并在responseTable移除
				responseFuture.setSendRequestOK(false);
				responseTable.remove(opaque);
				responseFuture.setCause(f.cause());
				//执行这个方法同时也会调用countDownLatch countDown()方法
				responseFuture.putResponse(null);
				log.warn("Failed to write a request command to {}, caused by underlying I/O operation failure", addr);
			});
			//使用countDownLatch来等待响应到达
			RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
			if (null == responseCommand) {
				if (responseFuture.isSendRequestOK()) {
					throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis, responseFuture.getCause());
				} else {
					throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
				}
			}
			return responseCommand;
		} finally {
			this.responseTable.remove(opaque);
		}
	}

	public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
		long beginStartTime = System.currentTimeMillis();
		//相当于requestId，每个请求都会生成一个唯一id，每次加一
		final int opaque = request.getOpaque();
		//信号量流控
		boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
		if (acquired) {
			final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
			//发生了任何阻塞操作后都要检查是否超时
			long costTime = System.currentTimeMillis() - beginStartTime;
			if (timeoutMillis < costTime) {
				once.release();
				throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
			}

			final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);
			this.responseTable.put(opaque, responseFuture);
			try {
				// 使用Netty的Channel发送请求数据到服务端
				channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
					if (f.isSuccess()) {
						responseFuture.setSendRequestOK(true);
						return;
					}
					requestFail(opaque);
					log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
				});
			} catch (Exception e) {
				responseFuture.release();
				log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
				throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
			}
		} else {
			if (timeoutMillis <= 0) {
				throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
			} else {
				String info = String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", timeoutMillis, this.semaphoreAsync.getQueueLength(), this.semaphoreAsync.availablePermits());
				log.warn(info);
				throw new RemotingTimeoutException(info);
			}
		}
	}

	private void requestFail(final int opaque) {
		ResponseFuture responseFuture = responseTable.remove(opaque);
		if (responseFuture != null) {
			responseFuture.setSendRequestOK(false);
			responseFuture.putResponse(null);
			try {
				executeInvokeCallback(responseFuture);
			} catch (Throwable e) {
				log.warn("execute callback in requestFail, and callback throw", e);
			} finally {
				responseFuture.release();
			}
		}
	}

	/**
	 * mark the request of the specified channel as fail and to invoke fail callback immediately
	 *
	 * @param channel the channel which is close already
	 */
	protected void failFast(final Channel channel) {
		for (Entry<Integer, ResponseFuture> entry : responseTable.entrySet()) {
			if (entry.getValue().getChannel() == channel) {
				Integer opaque = entry.getKey();
				if (opaque != null) {
					requestFail(opaque);
				}
			}
		}
	}
	//通过连接（channel）发送数据
	public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
		//在请求头上的flag标记为oneway请求
		request.markOnewayRPC();
		//获取信号量，保证不会系统不会承受过多请求
		boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
		if (acquired) {
			final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
			try {
				channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
					//真正发送完成后，释放锁
					once.release();
					if (!f.isSuccess()) {
						log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
					}
				});
			} catch (Exception e) {
				once.release();
				log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
				throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
			}
		} else {
			//超出请求数
			if (timeoutMillis <= 0) {
				throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
			} else {
				String info = String.format("invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreOnewayValue: %d", timeoutMillis, this.semaphoreOneway.getQueueLength(), this.semaphoreOneway.availablePermits());
				log.warn(info);
				throw new RemotingTimeoutException(info);
			}
		}
	}

	class NettyEventExecutor extends ServiceThread {
		private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<>();

		public void putNettyEvent(final NettyEvent event) {
			int currentSize = this.eventQueue.size();
			int maxSize = 10000;
			if (currentSize <= maxSize) {
				this.eventQueue.add(event);
			} else {
				log.warn("event queue size [{}] over the limit [{}], so drop this event {}", currentSize, maxSize, event.toString());
			}
		}

		@Override
		public void run() {
			log.info(this.getServiceName() + " service started");

			final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

			while (!this.isStopped()) {
				try {
					NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
					if (event != null && listener != null) {
						switch (event.getType()) {
						case IDLE:
							listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
							break;
						case CLOSE:
							listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
							break;
						case CONNECT:
							listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
							break;
						case EXCEPTION:
							listener.onChannelException(event.getRemoteAddr(), event.getChannel());
							break;
						default:
							break;

						}
					}
				} catch (Exception e) {
					log.warn(this.getServiceName() + " service has exception. ", e);
				}
			}

			log.info(this.getServiceName() + " service end");
		}

		@Override
		public String getServiceName() {
			return NettyEventExecutor.class.getSimpleName();
		}
	}
}
