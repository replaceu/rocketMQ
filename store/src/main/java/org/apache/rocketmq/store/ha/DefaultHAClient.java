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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.DefaultMessageStore;

public class DefaultHAClient extends ServiceThread implements HAClient {

	/**
	 * Report header buffer size. Schema: slaveMaxOffset. Format:
	 *
	 * <pre>
	 * ┌───────────────────────────────────────────────┐
	 * │                  slaveMaxOffset               │
	 * │                    (8bytes)                   │
	 * ├───────────────────────────────────────────────┤
	 * │                                               │
	 * │                  Report Header                │
	 * </pre>
	 * <p>
	 */
	public static final int REPORT_HEADER_SIZE = 8;

	private static final Logger				log						= LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	private static final int				READ_MAX_BUFFER_SIZE	= 1024 * 1024 * 4;
	private final AtomicReference<String>	masterHaAddress			= new AtomicReference<>();
	private final AtomicReference<String>	masterAddress			= new AtomicReference<>();
	private final ByteBuffer				reportOffset			= ByteBuffer.allocate(REPORT_HEADER_SIZE);
	private SocketChannel					socketChannel;
	private Selector						selector;
	/**
	 * last time that slave reads date from master.
	 */
	private long							lastReadTimestamp		= System.currentTimeMillis();
	/**
	 * last time that slave reports offset to master.
	 */
	private long							lastWriteTimestamp		= System.currentTimeMillis();

	private long						currentReportedOffset	= 0;
	private int							dispatchPosition		= 0;
	private ByteBuffer					byteBufferRead			= ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
	private ByteBuffer					byteBufferBackup		= ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
	private DefaultMessageStore			defaultMessageStore;
	private volatile HAConnectionState	currentState			= HAConnectionState.READY;
	private FlowMonitor					flowMonitor;

	public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
		this.selector = NetworkUtil.openSelector();
		this.defaultMessageStore = defaultMessageStore;
		this.flowMonitor = new FlowMonitor(defaultMessageStore.getMessageStoreConfig());
	}

	public void updateHaMasterAddress(final String newAddr) {
		String currentAddr = this.masterHaAddress.get();
		if (masterHaAddress.compareAndSet(currentAddr, newAddr)) {
			log.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddr);
		}
	}

	public void updateMasterAddress(final String newAddr) {
		String currentAddr = this.masterAddress.get();
		if (masterAddress.compareAndSet(currentAddr, newAddr)) {
			log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
		}
	}

	public String getHaMasterAddress() {
		return this.masterHaAddress.get();
	}

	public String getMasterAddress() {
		return this.masterAddress.get();
	}

	/**
	 * 判断是否需要向Master汇报已拉取消息偏移量
	 * @return
	 */
	private boolean isTimeToReportOffset() {
		//当前时间减去最后一次写时间
		long interval = defaultMessageStore.now() - this.lastWriteTimestamp;
		//大于发送心跳时间间隔
		return interval > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
	}

	private boolean reportSlaveMaxOffset(final long maxOffset) {
		//发送8字节的请求
		this.reportOffset.position(0);
		this.reportOffset.limit(REPORT_HEADER_SIZE);
		//最大偏移量
		this.reportOffset.putLong(maxOffset);
		this.reportOffset.position(0);
		this.reportOffset.limit(REPORT_HEADER_SIZE);
		//如果需要向Master反馈当前拉取偏移量，则向Master发送一个8字节的请求，请求包中包含的数据为当前Broker消息文件的最大偏移量。
		//如果还有剩余空间并且循环次数小于3
		for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
			try {
				//发送上报的偏移量
				this.socketChannel.write(this.reportOffset);
			} catch (IOException e) {
				log.error(this.getServiceName() + "reportSlaveMaxOffset this.socketChannel.write exception", e);
				return false;
			}
		}
		//最后一次写时间
		lastWriteTimestamp = this.defaultMessageStore.getSystemClock().now();
		//是否还有剩余空间
		return !this.reportOffset.hasRemaining();
	}

	private void reallocateByteBuffer() {
		int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
		if (remain > 0) {
			this.byteBufferRead.position(this.dispatchPosition);

			this.byteBufferBackup.position(0);
			this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
			this.byteBufferBackup.put(this.byteBufferRead);
		}

		this.swapByteBuffer();

		this.byteBufferRead.position(remain);
		this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
		this.dispatchPosition = 0;
	}

	private void swapByteBuffer() {
		ByteBuffer tmp = this.byteBufferRead;
		this.byteBufferRead = this.byteBufferBackup;
		this.byteBufferBackup = tmp;
	}

	//处理读事件
	private boolean processReadEvent() {
		int readSizeZeroTimes = 0;
		//判断是否还有剩余
		while (this.byteBufferRead.hasRemaining()) {
			try {
				//读取到缓存中
				int readSize = this.socketChannel.read(this.byteBufferRead);
				//如果读取的大小大于0
				if (readSize > 0) {
					flowMonitor.addByteCountTransferred(readSize);
					//重置读取到0字节的次数
					readSizeZeroTimes = 0;
					//分发读请求
					boolean result = this.dispatchReadRequest();
					if (!result) {
						log.error("HAClient, dispatchReadRequest error");
						return false;
					}
					lastReadTimestamp = System.currentTimeMillis();
				} else if (readSize == 0) {
					//如果连续3次从网络通道读取到0个字节，则结束本次读，返回 true
					if (++readSizeZeroTimes >= 3) {
						break;
					}
				} else {
					log.info("HAClient, processReadEvent read socket < 0");
					return false;
				}
			} catch (IOException e) {
				log.info("HAClient, processReadEvent read socket exception", e);
				return false;
			}
		}

		return true;
	}

	/**
	 * dispatchReadRequest方法将读取到的数据一条一条解析，并且落盘保存到本地。但是读取的数据可能不是完整的，
	 * 所以要判断读取的数据是否完整，消息包括消息头和消息体，消息头12字节长度，包括物理偏移量与消息的长度。
	 * 首先判断读取到数据的长度是否大于消息头部长度，如果小于说明读取的数据是不完整的，则判断byteBufferRead是否还有剩余空间，
	 * 并且将readByteBuffer中剩余的有效数据先复制到readByteBufferBak,然后交换readByteBuffer与readByteBufferBak。
	 *
	 * 如果读取的数据的长度大于消息头部长度，则判断master和slave的偏移量是否相等，如果slave的最大物理偏移量与master给的偏移量不相等，则返回false。
	 * 在后续的处理中，返回false，将会关闭与master的连接。如果读取的数据长度大于等于消息头长读与消息体长度，
	 * 说明读取的数据是包好完整消息的，将消息体的内容从byteBufferRead中读取出来，并且将消息保存到commitLog文件中。
	 *
	 * 总结起来，HAClient类的作用就是Slave上报自己的最大偏移量，以及处理从master拉取过来的数据并落盘保存起来。
	 * @return
	 */
	private boolean dispatchReadRequest() {
		//记录当前byteBufferRead的当前指针
		int readSocketPos = this.byteBufferRead.position();
		while (true) {
			//当前指针减去本次己处理读缓存区的指针
			int diff = this.byteBufferRead.position() - this.dispatchPosition;
			//是否大于头部长度，是否包含头部
			if (diff >= DefaultHAConnection.TRANSFER_HEADER_SIZE) {
				//master的物理偏移量
				long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
				//消息的长度
				int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);
				//slave偏移量
				long slavePhyOffset = this.defaultMessageStore.getMaxPhyOffset();
				//如果slave的最大物理偏移量与master给的偏移量不相等，则返回false
				//从后面的处理逻辑来看，返回false,将会关闭与master的连接，在Slave本次周期内将不会再参与主从同步了。
				if (slavePhyOffset != 0) {
					if (slavePhyOffset != masterPhyOffset) {
						log.error("master pushed offset not equal the max phy offset in slave, SLAVE: " + slavePhyOffset + " MASTER: " + masterPhyOffset);
						return false;
					}
				}
				//包含完整的信息
				if (diff >= (DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize)) {
					//读取消息到缓冲区
					byte[] bodyData = byteBufferRead.array();
					int dataStart = this.dispatchPosition + DefaultHAConnection.TRANSFER_HEADER_SIZE;
					//添加到commit log 文件
					this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData, dataStart, bodySize);
					this.byteBufferRead.position(readSocketPos);
					//当前的已读缓冲区指针
					this.dispatchPosition += DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize;
					//上报slave最大的复制偏移量
					if (!reportSlaveMaxOffsetPlus()) { return false; }

					continue;
				}
			}
			//没有包含完整的消息，
			//其核心思想是将readByteBuffer中剩余的有效数据先复制到readByteBufferBak,然后交换readByteBuffer与readByteBufferBak
			if (!this.byteBufferRead.hasRemaining()) {
				this.reallocateByteBuffer();
			}

			break;
		}

		return true;
	}

	private boolean reportSlaveMaxOffsetPlus() {
		boolean result = true;
		long currentPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
		if (currentPhyOffset > this.currentReportedOffset) {
			this.currentReportedOffset = currentPhyOffset;
			result = this.reportSlaveMaxOffset(this.currentReportedOffset);
			if (!result) {
				this.closeMaster();
				log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
			}
		}

		return result;
	}

	public void changeCurrentState(HAConnectionState currentState) {
		log.info("change state to {}", currentState);
		this.currentState = currentState;
	}

	/**
	 * connectMaster方法是slave主动连接master，首先判断该slave与master的socketChannel是否等于null，
	 * 如果等于socketChannel等于null，则创建slave与master的连接，然后注册OP_READ的事件，
	 * 并初始化currentReportedOffset 为commitLog文件的最大偏移量。
	 * 如果Broker启动的时候，配置的角色是Slave时，但是masterAddress没有配置，那么就不会连接master。
	 * 最后该方法返回是否成功连接上master
	 * @return
	 * @throws ClosedChannelException
	 */
	public boolean connectMaster() throws ClosedChannelException {
		if (null == socketChannel) {
			String addr = this.masterHaAddress.get();
			if (addr != null) {
				SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
				this.socketChannel = RemotingHelper.connect(socketAddress);
				if (this.socketChannel != null) {
					this.socketChannel.register(this.selector, SelectionKey.OP_READ);
					log.info("HAClient connect to master {}", addr);
					this.changeCurrentState(HAConnectionState.TRANSFER);
				}
			}
			//设置当前的复制进度为commitlog文件的最大偏移量
			this.currentReportedOffset = this.defaultMessageStore.getMaxPhyOffset();
			//最后一次写的时间
			this.lastReadTimestamp = System.currentTimeMillis();
		}

		return this.socketChannel != null;
	}

	public void closeMaster() {
		if (null != this.socketChannel) {
			try {

				SelectionKey sk = this.socketChannel.keyFor(this.selector);
				if (sk != null) {
					sk.cancel();
				}

				this.socketChannel.close();

				this.socketChannel = null;

				log.info("HAClient close connection with master {}", this.masterHaAddress.get());
				this.changeCurrentState(HAConnectionState.READY);
			} catch (IOException e) {
				log.warn("closeMaster exception. ", e);
			}

			this.lastReadTimestamp = 0;
			this.dispatchPosition = 0;

			this.byteBufferBackup.position(0);
			this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

			this.byteBufferRead.position(0);
			this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
		}
	}

	@Override
	public void run() {
		log.info(this.getServiceName() + " service started");

		this.flowMonitor.start();

		while (!this.isStopped()) {
			try {
				switch (this.currentState) {
				case SHUTDOWN:
					this.flowMonitor.shutdown(true);
					return;
				case READY:
					if (!this.connectMaster()) {
						log.warn("HAClient connect to master {} failed", this.masterHaAddress.get());
						this.waitForRunning(1000 * 5);
					}
					continue;
				case TRANSFER:
					if (!transferFromMaster()) {
						closeMasterAndWait();
						continue;
					}
					break;
				default:
					this.waitForRunning(1000 * 2);
					continue;
				}
				long interval = this.defaultMessageStore.now() - this.lastReadTimestamp;
				if (interval > this.defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
					log.warn("AutoRecoverHAClient, housekeeping, found this connection[" + this.masterHaAddress + "] expired, " + interval);
					this.closeMaster();
					log.warn("AutoRecoverHAClient, master not response some time, so close connection");
				}
			} catch (Exception e) {
				log.warn(this.getServiceName() + " service has exception. ", e);
				this.closeMasterAndWait();
			}
		}

		this.flowMonitor.shutdown(true);
		log.info(this.getServiceName() + " service end");
	}

	private boolean transferFromMaster() throws IOException {
		boolean result;
		//当前时间减去最后一些写时间，大于发送心跳时间间隔，那么就需要向master上报已拉取的消息偏移量
		if (this.isTimeToReportOffset()) {
			log.info("Slave report current offset {}", this.currentReportedOffset);
			result = this.reportSlaveMaxOffset(this.currentReportedOffset);
			if (!result) { return false; }
		}
		//进行事件选择，其执行间隔为 1s
		this.selector.select(1000);
		//处理读事件
		result = this.processReadEvent();
		if (!result) { return false; }
		//上报slave最大的偏移量
		return reportSlaveMaxOffsetPlus();
	}

	public void closeMasterAndWait() {
		this.closeMaster();
		this.waitForRunning(1000 * 5);
	}

	public long getLastWriteTimestamp() {
		return this.lastWriteTimestamp;
	}

	public long getLastReadTimestamp() {
		return lastReadTimestamp;
	}

	@Override
	public HAConnectionState getCurrentState() {
		return currentState;
	}

	@Override
	public long getTransferredByteInSecond() {
		return flowMonitor.getTransferredByteInSecond();
	}

	@Override
	public void shutdown() {
		this.changeCurrentState(HAConnectionState.SHUTDOWN);
		this.flowMonitor.shutdown();
		super.shutdown();

		closeMaster();
		try {
			this.selector.close();
		} catch (IOException e) {
			log.warn("Close the selector of AutoRecoverHAClient error, ", e);
		}
	}

	@Override
	public String getServiceName() {
		if (this.defaultMessageStore != null && this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) { return this.defaultMessageStore.getBrokerIdentity().getIdentifier() + DefaultHAClient.class.getSimpleName(); }
		return DefaultHAClient.class.getSimpleName();
	}
}
