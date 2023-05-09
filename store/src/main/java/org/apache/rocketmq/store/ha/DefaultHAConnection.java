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

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class DefaultHAConnection implements HAConnection {

	/**
	 * Transfer Header buffer size. Schema: physic offset and body size. Format:
	 *
	 * <pre>
	 * ┌───────────────────────────────────────────────┬───────────────────────┐
	 * │                  physicOffset                 │         bodySize      │
	 * │                    (8bytes)                   │         (4bytes)      │
	 * ├───────────────────────────────────────────────┴───────────────────────┤
	 * │                                                                       │
	 * │                           Transfer Header                             │
	 * </pre>
	 * <p>
	 */
	public static final int TRANSFER_HEADER_SIZE = 8 + 4;

	private static final Logger			log					= LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	private final DefaultHAService		haService;
	private final SocketChannel			socketChannel;
	private final String				clientAddress;
	private WriteSocketService			writeSocketService;
	private ReadSocketService			readSocketService;
	private volatile HAConnectionState	currentState		= HAConnectionState.TRANSFER;
	private volatile long				slaveRequestOffset	= -1;
	private volatile long				slaveAckOffset		= -1;
	private FlowMonitor					flowMonitor;

	public DefaultHAConnection(final DefaultHAService haService, final SocketChannel socketChannel) throws IOException {
		this.haService = haService;
		this.socketChannel = socketChannel;
		this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
		this.socketChannel.configureBlocking(false);
		this.socketChannel.socket().setSoLinger(false, -1);
		this.socketChannel.socket().setTcpNoDelay(true);
		if (NettySystemConfig.socketSndbufSize > 0) {
			this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
		}
		if (NettySystemConfig.socketRcvbufSize > 0) {
			this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
		}
		this.writeSocketService = new WriteSocketService(this.socketChannel);
		this.readSocketService = new ReadSocketService(this.socketChannel);
		this.haService.getConnectionCount().incrementAndGet();
		this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
	}

	public void start() {
		changeCurrentState(HAConnectionState.TRANSFER);
		this.flowMonitor.start();
		this.readSocketService.start();
		this.writeSocketService.start();
	}

	public void shutdown() {
		changeCurrentState(HAConnectionState.SHUTDOWN);
		this.writeSocketService.shutdown(true);
		this.readSocketService.shutdown(true);
		this.flowMonitor.shutdown(true);
		this.close();
	}

	public void close() {
		if (this.socketChannel != null) {
			try {
				this.socketChannel.close();
			} catch (IOException e) {
				log.error("", e);
			}
		}
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}

	public void changeCurrentState(HAConnectionState currentState) {
		log.info("change state to {}", currentState);
		this.currentState = currentState;
	}

	@Override
	public HAConnectionState getCurrentState() {
		return currentState;
	}

	@Override
	public String getClientAddress() {
		return this.clientAddress;
	}

	@Override
	public long getSlaveAckOffset() {
		return slaveAckOffset;
	}

	public long getTransferredByteInSecond() {
		return this.flowMonitor.getTransferredByteInSecond();
	}

	public long getTransferFromWhere() {
		return writeSocketService.getNextTransferFromWhere();
	}

	class ReadSocketService extends ServiceThread {
		private static final int	READ_MAX_BUFFER_SIZE	= 1024 * 1024;
		private final Selector		selector;
		private final SocketChannel	socketChannel;
		private final ByteBuffer	byteBufferRead			= ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
		private int					processPosition			= 0;
		private volatile long		lastReadTimestamp		= System.currentTimeMillis();

		public ReadSocketService(final SocketChannel socketChannel) throws IOException {
			this.selector = NetworkUtil.openSelector();
			this.socketChannel = socketChannel;
			this.socketChannel.register(this.selector, SelectionKey.OP_READ);
			this.setDaemon(true);
		}

		@Override
		public void run() {
			log.info(this.getServiceName() + " service started");

			while (!this.isStopped()) {
				try {
					this.selector.select(1000);
					//todo：处理读事件
					boolean ok = this.processReadEvent();
					if (!ok) {
						log.error("processReadEvent error");
						break;
					}
					//当前时间戳减去最后一次读取时间戳
					long interval = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
					//大于长连接保持的时间
					if (interval > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
						log.warn("ha housekeeping, found this connection[" + DefaultHAConnection.this.clientAddress + "] expired, " + interval);
						break;
					}
				} catch (Exception e) {
					log.error(this.getServiceName() + " service has exception.", e);
					break;
				}
			}

			changeCurrentState(HAConnectionState.SHUTDOWN);
			//停止该读线程
			this.makeStop();
			//停止写线程
			writeSocketService.makeStop();
			//删除连接
			haService.removeConnection(DefaultHAConnection.this);
			//连接数减一
			DefaultHAConnection.this.haService.getConnectionCount().decrementAndGet();

			SelectionKey sk = this.socketChannel.keyFor(this.selector);
			if (sk != null) {
				sk.cancel();
			}
			//关闭选择器
			try {
				this.selector.close();
				this.socketChannel.close();
			} catch (IOException e) {
				log.error("", e);
			}

			flowMonitor.shutdown(true);

			log.info(this.getServiceName() + " service end");
		}

		@Override
		public String getServiceName() {
			if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) { return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + ReadSocketService.class.getSimpleName(); }
			return ReadSocketService.class.getSimpleName();
		}

		private boolean processReadEvent() {
			//读取的字节数为0的次数
			int readSizeZeroTimes = 0;
			//缓冲区没有剩余空间，还原
			if (!this.byteBufferRead.hasRemaining()) {
				this.byteBufferRead.flip();
				this.processPosition = 0;
			}
			//剩余空间
			while (this.byteBufferRead.hasRemaining()) {
				try {
					//处理网络读
					int readSize = this.socketChannel.read(this.byteBufferRead);
					//读取的字节大于0
					if (readSize > 0) {
						readSizeZeroTimes = 0;
						//最后一次读时间戳
						this.lastReadTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
						//当前位移减去已处理数据位移大于等于8，表明收到从服务器一条拉取消息请求。读取从服务器已拉取偏移量
						if ((this.byteBufferRead.position() - this.processPosition) >= DefaultHAClient.REPORT_HEADER_SIZE) {
							int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % DefaultHAClient.REPORT_HEADER_SIZE);
							//消息长度，减去8个字节（消息物理偏移量）
							long readOffset = this.byteBufferRead.getLong(pos - 8);
							this.processPosition = pos;
							//从服务器反馈已拉取完成的数据偏移量
							DefaultHAConnection.this.slaveAckOffset = readOffset;
							if (DefaultHAConnection.this.slaveRequestOffset < 0) {
								DefaultHAConnection.this.slaveRequestOffset = readOffset;
								log.info("slave[" + DefaultHAConnection.this.clientAddress + "] request offset " + readOffset);
							}
							//唤醒线程去判断自己的关注的消息是否已经传输完成
							DefaultHAConnection.this.haService.notifyTransferSome(DefaultHAConnection.this.slaveAckOffset);
						}
					} else if (readSize == 0) {
						//如果读取到的字节数等于0，则重复三次，否则结束本次读请求处理
						if (++readSizeZeroTimes >= 3) {
							break;
						}
					} else {
						//如果读取到的字节数小于0，表示连接被断开，返回false，后续会断开该连接
						log.error("read socket[" + DefaultHAConnection.this.clientAddress + "] < 0");
						return false;
					}
				} catch (IOException e) {
					log.error("processReadEvent exception", e);
					return false;
				}
			}

			return true;
		}
	}

	class WriteSocketService extends ServiceThread {
		private final Selector				selector;
		private final SocketChannel			socketChannel;
		private final ByteBuffer			byteBufferHeader		= ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
		private long						nextTransferFromWhere	= -1;
		private SelectMappedBufferResult	selectMappedBufferResult;
		private boolean						lastWriteOver			= true;
		private long						lastPrintTimestamp		= System.currentTimeMillis();
		private long						lastWriteTimestamp		= System.currentTimeMillis();

		public WriteSocketService(final SocketChannel socketChannel) throws IOException {
			this.selector = NetworkUtil.openSelector();
			this.socketChannel = socketChannel;
			this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
			this.setDaemon(true);
		}

		@Override
		public void run() {
			log.info(this.getServiceName() + " service started");

			while (!this.isStopped()) {
				try {
					this.selector.select(1000);
					//从服务器请求拉取数据的偏移量等于-1，说明Master还未收到从服务器的拉取请求，放弃本次事件处理
					if (-1 == DefaultHAConnection.this.slaveRequestOffset) {
						Thread.sleep(10);
						continue;
					}
					//下一次传输的物理偏移量等于-1，表示初次进行数据传输
					if (-1 == this.nextTransferFromWhere) {
						//如果slaveRequestOffset为0，则从当前commitLog文件最大偏移量开始传输，否则根据从服务器的拉取请求偏移量开始传输
						if (0 == DefaultHAConnection.this.slaveRequestOffset) {
							//master位移
							long masterOffset = DefaultHAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
							//master位移减去MappedFile文件的大小
							masterOffset = masterOffset - (masterOffset % DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getMappedFileSizeCommitLog());

							if (masterOffset < 0) {
								masterOffset = 0;
							}

							this.nextTransferFromWhere = masterOffset;
						} else {
							this.nextTransferFromWhere = DefaultHAConnection.this.slaveRequestOffset;
						}

						log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + DefaultHAConnection.this.clientAddress + "], and slave request " + DefaultHAConnection.this.slaveRequestOffset);
					}
					//上一次数据是否全部传输给客户端
					if (this.lastWriteOver) {
						//当前时间戳减去最后一次写时间戳
						long interval = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
						//大于发送心跳时间间隔的时间
						if (interval > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
							// Build Header
							this.byteBufferHeader.position(0);
							//消息头长度
							this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
							this.byteBufferHeader.putLong(this.nextTransferFromWhere);
							this.byteBufferHeader.putInt(0);
							this.byteBufferHeader.flip();
							//传输数据
							this.lastWriteOver = this.transferData();
							if (!this.lastWriteOver) continue;
						}
					} else {
						//还没有传输完毕
						this.lastWriteOver = this.transferData();
						if (!this.lastWriteOver) continue;
					}

					SelectMappedBufferResult selectResult = DefaultHAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
					if (selectResult != null) {
						int size = selectResult.getSize();
						if (size > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
							size = DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
						}

						int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
						if (size > canTransferMaxBytes) {
							if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
								log.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s", String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0), String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
								lastPrintTimestamp = System.currentTimeMillis();
							}
							size = canTransferMaxBytes;
						}

						long thisOffset = this.nextTransferFromWhere;
						this.nextTransferFromWhere += size;

						selectResult.getByteBuffer().limit(size);
						this.selectMappedBufferResult = selectResult;

						// Build Header
						this.byteBufferHeader.position(0);
						this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
						this.byteBufferHeader.putLong(thisOffset);
						this.byteBufferHeader.putInt(size);
						this.byteBufferHeader.flip();

						this.lastWriteOver = this.transferData();
					} else {

						DefaultHAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
					}
				} catch (Exception e) {

					DefaultHAConnection.log.error(this.getServiceName() + " service has exception.", e);
					break;
				}
			}

			DefaultHAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

			if (this.selectMappedBufferResult != null) {
				this.selectMappedBufferResult.release();
			}

			changeCurrentState(HAConnectionState.SHUTDOWN);

			this.makeStop();

			readSocketService.makeStop();

			haService.removeConnection(DefaultHAConnection.this);

			SelectionKey sk = this.socketChannel.keyFor(this.selector);
			if (sk != null) {
				sk.cancel();
			}

			try {
				this.selector.close();
				this.socketChannel.close();
			} catch (IOException e) {
				DefaultHAConnection.log.error("", e);
			}

			flowMonitor.shutdown(true);

			DefaultHAConnection.log.info(this.getServiceName() + " service end");
		}

		private boolean transferData() throws Exception {
			int writeSizeZeroTimes = 0;
			// Write Header
			//Write Header传输头信息
			while (this.byteBufferHeader.hasRemaining()) {
				//写入头信息给slave服务器
				int writeSize = this.socketChannel.write(this.byteBufferHeader);
				//写入大小大于0
				if (writeSize > 0) {
					flowMonitor.addByteCountTransferred(writeSize);
					writeSizeZeroTimes = 0;
					this.lastWriteTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
				} else if (writeSize == 0) {
					//写入大小大于等于3，则停止向slave信息
					if (++writeSizeZeroTimes >= 3) {
						break;
					}
				} else {
					throw new Exception("ha master write header error < 0");
				}
			}

			if (null == this.selectMappedBufferResult) { return !this.byteBufferHeader.hasRemaining(); }
			writeSizeZeroTimes = 0;
			// Write Body
			//消息头缓冲没有剩余空间
			if (!this.byteBufferHeader.hasRemaining()) {
				//还有剩余数据
				while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
					//将数据写入到从服务器
					int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
					if (writeSize > 0) {
						writeSizeZeroTimes = 0;
						this.lastWriteTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
					} else if (writeSize == 0) {
						if (++writeSizeZeroTimes >= 3) {
							break;
						}
					} else {
						throw new Exception("ha master write body error < 0");
					}
				}
			}
			//头信息和消息体都没有剩余空间
			boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();
			//没有剩余空间，则释放mapped缓存
			if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
				this.selectMappedBufferResult.release();
				this.selectMappedBufferResult = null;
			}

			return result;
		}

		@Override
		public String getServiceName() {
			if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) { return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + WriteSocketService.class.getSimpleName(); }
			return WriteSocketService.class.getSimpleName();
		}

		@Override
		public void shutdown() {
			super.shutdown();
		}

		public long getNextTransferFromWhere() {
			return nextTransferFromWhere;
		}
	}
}
