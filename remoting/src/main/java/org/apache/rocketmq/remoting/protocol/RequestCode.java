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

package org.apache.rocketmq.remoting.protocol;

public class RequestCode {
    //发送消息的操作码
    public static final int SEND_MESSAGE = 10;
    //拉取消息的操作码
    public static final int PULL_MESSAGE = 11;
    //查询消息(所在topic, 需要的 key, 最大数量, 开始偏移量, 结束偏移量)的操作码
    public static final int QUERY_MESSAGE = 12;
    //查询Broker偏移量(未使用)的操作码
    public static final int QUERY_BROKER_OFFSET = 13;
    /**
     *查询消费者偏移量
     * 消费者会将偏移量存储在内存中,当使用主从架构时,会默认由主 Broker 负责读与写
     * 为避免消息堆积,堆积消息超过指定的值时,会由从服务器来接管读,但会导致消费进度问题
     * 所以主从消费进度的一致性由从服务器主动上报和消费者内存进度优先来保证
     */
    //查询消费者自己的偏移量的操作码
    public static final int QUERY_CONSUMER_OFFSET = 14;
    //提交自己消费者的偏移量
    public static final int UPDATE_CONSUMER_OFFSET = 15;
    //创建或更新Topic
    public static final int UPDATE_AND_CREATE_TOPIC = 17;
    //获取所有的Topic信息
    public static final int GET_ALL_TOPIC_CONFIG = 21;
    public static final int GET_TOPIC_CONFIG_LIST = 22;
    public static final int GET_TOPIC_NAME_LIST = 23;
    //更新Broker配置
    public static final int UPDATE_BROKER_CONFIG = 25;
    //获取Broker配置
    public static final int GET_BROKER_CONFIG = 26;
    public static final int TRIGGER_DELETE_FILES = 27;
    // 获取Broker运行时信息
    public static final int GET_BROKER_RUNTIME_INFO = 28;
    //通过时间戳查找偏移量
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;
    //获取最大偏移量
    public static final int GET_MAX_OFFSET = 30;
    //获取最小偏移量
    public static final int GET_MIN_OFFSET = 31;

    public static final int GET_EARLIEST_MSG_STORETIME = 32;
    //通过消息ID查询消息
    public static final int VIEW_MESSAGE_BY_ID = 33;
    //心跳消息
    public static final int HEART_BEAT = 34;
    //注销客户端
    public static final int UNREGISTER_CLIENT = 35;
    //报告消费失败(一段时间后重试) (Deprecated)
    public static final int CONSUMER_SEND_MSG_BACK = 36;
    //事务结果(可能是 commit 或 rollback)
    public static final int END_TRANSACTION = 37;
    // 通过消费者组获取消费者列表
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;
    //检查事务状态; Broker对于事务的未知状态的回查操作
    public static final int CHECK_TRANSACTION_STATE = 39;
    //通知消费者的ID已经被更改
    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;
    //批量锁定 Queue (reBalance使用)
    public static final int LOCK_BATCH_MQ = 41;
    // 解锁 Queue
    public static final int UNLOCK_BATCH_MQ = 42;
    //获得该 Broker 上的所有的消费者偏移量
    public static final int GET_ALL_CONSUMER_OFFSET = 43;
    //获得延迟Topic上的偏移量
    public static final int GET_ALL_DELAY_OFFSET = 45;
    //检查客户端配置
    public static final int CHECK_CLIENT_CONFIG = 46;

    public static final int GET_CLIENT_CONFIG = 47;
    //更新或创建ACL
    public static final int UPDATE_AND_CREATE_ACL_CONFIG = 50;
    //删除ACL配置
    public static final int DELETE_ACL_CONFIG = 51;
    //获取Broker集群的ACL信息
    public static final int GET_BROKER_CLUSTER_ACL_INFO = 52;
    //更新全局白名单
    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53;
    //获取Broker集群的ACL配置
    public static final int GET_BROKER_CLUSTER_ACL_CONFIG = 54;

    public static final int GET_TIMER_CHECK_POINT = 60;

    public static final int GET_TIMER_METRICS = 61;

    public static final int POP_MESSAGE = 200050;
    public static final int ACK_MESSAGE = 200051;
    public static final int PEEK_MESSAGE = 200052;
    public static final int CHANGE_MESSAGE_INVISIBLETIME = 200053;
    public static final int NOTIFICATION = 200054;
    public static final int POLLING_INFO = 200055;
    //放入键值配置
    public static final int PUT_KV_CONFIG = 100;
    //获取键值配置
    public static final int GET_KV_CONFIG = 101;
    //删除键值配置
    public static final int DELETE_KV_CONFIG = 102;
    //注册 Broker
    public static final int REGISTER_BROKER = 103;
    //注销 Broker
    public static final int UNREGISTER_BROKER = 104;
    //获取指定 Topic 的路由信息
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;
    //获取 Broker 的集群信息
    public static final int GET_BROKER_CLUSTER_INFO = 106;
    //更新或创建订阅组
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    //获取所有订阅组的配置
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    //获取 Topic 的度量指标
    public static final int GET_TOPIC_STATS_INFO = 202;
    //获取消费者在线列表(rpc)
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;
    //获取生产者在线列表
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;
    public static final int WIPE_WRITE_PERM_OF_BROKER = 205;
    //从 NameSrv 获取所有 Topic
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;
    //删除订阅组
    public static final int DELETE_SUBSCRIPTIONGROUP = 207;
    //获取消费者的度量指标
    public static final int GET_CONSUME_STATS = 208;

    public static final int SUSPEND_CONSUMER = 209;

    public static final int RESUME_CONSUMER = 210;
    public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;

    public static final int ADJUST_CONSUMER_THREAD_POOL = 213;

    public static final int WHO_CONSUME_THE_MESSAGE = 214;
    //删除Broker中的Topic
    public static final int DELETE_TOPIC_IN_BROKER = 215;
    //删除NameSrv中的Topic
    public static final int DELETE_TOPIC_IN_NAMESRV = 216;
    public static final int REGISTER_TOPIC_IN_NAMESRV = 217;
    //获取键值列表
    public static final int GET_KVLIST_BY_NAMESPACE = 219;
    //重置消费者的消费进度
    public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;
    //从消费者中获取消费者的度量指标
    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;
    //让Broker重置消费进度
    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;
    //让Broker更新消费者的度量信息
    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;
    //查询消息被谁消费
    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;
    //从集群中获取 Topic
    public static final int GET_TOPICS_BY_CLUSTER = 224;

    public static final int QUERY_TOPICS_BY_CONSUMER = 343;
    public static final int QUERY_SUBSCRIPTION_BY_CONSUMER = 345;
    //注册过滤器服务器
    public static final int REGISTER_FILTER_SERVER = 301;
    //注册消息过滤类
    public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;
    //查询消费时间
    public static final int QUERY_CONSUME_TIME_SPAN = 303;
    //从NameSrv中获取系统Topic
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    //从Broker中获取系统Topic
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;
    //清理过期的消费队列
    public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;
    //获取Consumer的运行时信息
    public static final int GET_CONSUMER_RUNNING_INFO = 307;
    //查询修正偏移量
    public static final int QUERY_CORRECTION_OFFSET = 308;
    //直接消费消息
    public static final int CONSUME_MESSAGE_DIRECTLY = 309;
    //发送消息(v2),优化网络数据包
    public static final int SEND_MESSAGE_V2 = 310;
    //单元化相关 topic
    public static final int GET_UNIT_TOPIC_LIST = 311;
    //获取含有单元化订阅组的Topic列表
    public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;
    //获取含有单元化订阅组的非单元化Topic列表
    public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;
    //克隆消费进度
    public static final int CLONE_GROUP_OFFSET = 314;
    //查询Broker上的度量信息
    public static final int VIEW_BROKER_STATS_DATA = 315;
    //清理未使用的Topic
    public static final int CLEAN_UNUSED_TOPIC = 316;
    //获取 broker 上的有关消费的度量信息
    public static final int GET_BROKER_CONSUME_STATS = 317;

    /**
     * update the config of name server
     */
    public static final int UPDATE_NAMESRV_CONFIG = 318;

    /**
     * get config from name server
     */
    public static final int GET_NAMESRV_CONFIG = 319;
    //发送批量消息
    public static final int SEND_BATCH_MESSAGE = 320;
    //查询消费的 Queue
    public static final int QUERY_CONSUME_QUEUE = 321;
    //查询数据版本
    public static final int QUERY_DATA_VERSION = 322;

    /**
     * resume logic of checking half messages that have been put in TRANS_CHECK_MAXTIME_TOPIC before
     */
    public static final int RESUME_CHECK_HALF_MESSAGE = 323;
    //回送消息
    public static final int SEND_REPLY_MESSAGE = 324;

    public static final int SEND_REPLY_MESSAGE_V2 = 325;
    //push回送消息到客户端
    public static final int PUSH_REPLY_MESSAGE_TO_CLIENT = 326;

    public static final int ADD_WRITE_PERM_OF_BROKER = 327;

    public static final int GET_TOPIC_CONFIG = 351;

    public static final int GET_SUBSCRIPTIONGROUP_CONFIG = 352;
    public static final int UPDATE_AND_GET_GROUP_FORBIDDEN = 353;

    public static final int LITE_PULL_MESSAGE = 361;

    public static final int QUERY_ASSIGNMENT = 400;
    public static final int SET_MESSAGE_REQUEST_MODE = 401;
    public static final int GET_ALL_MESSAGE_REQUEST_MODE = 402;

    public static final int UPDATE_AND_CREATE_STATIC_TOPIC = 513;

    public static final int GET_BROKER_MEMBER_GROUP = 901;

    public static final int ADD_BROKER = 902;

    public static final int REMOVE_BROKER = 903;

    public static final int BROKER_HEARTBEAT = 904;

    public static final int NOTIFY_MIN_BROKER_ID_CHANGE = 905;

    public static final int EXCHANGE_BROKER_HA_INFO = 906;

    public static final int GET_BROKER_HA_STATUS = 907;

    public static final int RESET_MASTER_FLUSH_OFFSET = 908;

    public static final int GET_ALL_PRODUCER_INFO = 328;

    public static final int DELETE_EXPIRED_COMMITLOG = 329;

    /**
     * Controller code
     */
    public static final int CONTROLLER_ALTER_SYNC_STATE_SET = 1001;

    public static final int CONTROLLER_ELECT_MASTER = 1002;

    public static final int CONTROLLER_REGISTER_BROKER = 1003;

    public static final int CONTROLLER_GET_REPLICA_INFO = 1004;

    public static final int CONTROLLER_GET_METADATA_INFO = 1005;

    public static final int CONTROLLER_GET_SYNC_STATE_DATA = 1006;

    public static final int GET_BROKER_EPOCH_CACHE = 1007;

    public static final int NOTIFY_BROKER_ROLE_CHANGED = 1008;

    /**
     * update the config of controller
     */
    public static final int UPDATE_CONTROLLER_CONFIG = 1009;

    /**
     * get config from controller
     */
    public static final int GET_CONTROLLER_CONFIG = 1010;

    /**
     * clean broker data
     */
    public static final int CLEAN_BROKER_DATA = 1011;

    public static final int CONTROLLER_GET_NEXT_BROKER_ID = 1012;

    public static final int CONTROLLER_APPLY_BROKER_ID = 1013;
}
