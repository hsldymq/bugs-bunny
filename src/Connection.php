<?php

namespace Archman\BugsBunny;

use Archman\Whisper\Helper;
use Bunny\Async\Client;
use Bunny\Channel;
use React\EventLoop\LoopInterface;

class Connection
{
    /**
     * @var LoopInterface
     */
    private $eventLoop;

    /**
     * @var callable
     */
    private $handler;

    /**
     * @var array
     */
    private $queues;

    /**
     * @var array 连接参数
     * [
     *      'host' => (string),
     *      'port' => (integer),
     *      'vhost' => (string),
     *      'user' => (string),
     *      'password' => (string),
     * ]
     */
    private $connectionOptions;

    /**
     * @var int 建立连接的数量
     */
    private $connectionNum;

    /**
     * @var array
     * [
     *      [
     *          'client' => $client,
     *          'channel' => $channel,
     *          'tags' => [$tag1, $tag2, ...],
     *      ],
     *      ...
     * ]
     */
    private $connections = [];

    /**
     * @var array
     * [
     *      $tag = $queue,
     *      ...
     * ]
     */
    private $queueMap = [];

    /**
     * @var bool
     */
    private $connected = false;

    /**
     * @var bool
     */
    private $paused = false;

    /**
     * Connection constructor.
     * @param array $options
     * [
     *      'host' => 'xxx',
     *      'port' => 123,
     *      'vhost' => 'yyy',
     *      'user' => 'zzz',
     *      'password' => 'uuu',
     *      'connections' => 1,                 // =1, 同时创建多少个连接. [可选]默认为1
     *      // 'reconnectOnError' => false / true, // [可选]默认为true
     *      // 'maxReconnectRetries => 1,          // >=1, 重连尝试次数,超过将抛出异常. [可选] 默认为3
     * ]
     * @param array $queues 队列名称列表
     */
    public function __construct(array $options, array $queues)
    {
        $this->queues = $queues;
        $this->connectionNum = max(1, $options['connections'] ?? 1);
        $this->connectionOptions = array_intersect_key($options, [
            'host' => true,
            'port' => true,
            'vhost' => true,
            'user' => true,
            'password' => true,
        ]);
    }

    public function connect(LoopInterface $eventLoop, callable $consumeHandler)
    {
        if ($this->connected) {
            // TODO reconnect
            return;
        }

        $this->eventLoop = $eventLoop;
        $this->handler = $consumeHandler;
        $this->connected = true;

        for ($i = 0; $i < $this->connectionNum; $i++) {
            $this->makeConnection();
        }
    }

    public function disconnect()
    {
        foreach ($this->connections as $index => $each) {
            /** @var Client $client */
            $client = $each['client'];
            $tags = $each['tags'];
            $client->disconnect()->done();
            unset($this->connections[$index]);
            foreach ($tags as $t) {
                unset($this->queueMap[$t]);
            }
        }
    }

//
//    /**
//     * @param array $queues
//     */
//    public function reconnect(LoopInterface $eventLoop, callable $consumeHandler)
//    {
//        $this->disconnect();
//        $this->connect($this->eventLoop, $this->handler);
//    }

    /**
     * 暂停消费队列消息.
     */
    public function pause()
    {
        if ($this->paused || !$this->connected) {
            return;
        }

        foreach ($this->connections as $index => $each) {
            $tags = $each['tags'];
            $this->unbindConsumer($each['channel'], $tags);
            foreach ($tags as $i => $t) {
                unset($this->connections[$index]['tags'][$i]);
                unset($this->queueMap[$t]);
            }
        }
        $this->paused = true;
    }

    /**
     * 恢复消费队列消息.
     */
    public function resume()
    {
        if (!$this->paused || !$this->connected) {
            return;
        }

        foreach ($this->connections as $index => $each) {
            $tags = $this->bindConsumer($each['channel'], $this->queues, $this->handler);
            $this->connections[$index]['tags'] = array_keys($tags);
            $this->queueMap = array_merge($this->queueMap, $tags);
        }
        $this->paused = false;
    }

    /**
     * @param string$tag
     *
     * @return string|null
     */
    public function getQueue(string $tag)
    {
        return $this->queueMap[$tag] ?? null;
    }

    private function makeConnection()
    {
        $onReject = function ($reason) {
            $msg = "Failed to connect AMQP server.";

            if (is_string($reason)) {
                $msg .= $reason;
            } else if ($reason instanceof \Throwable) {
                $prev = $reason;
                $msg .= $reason->getMessage();
            }

            throw new \Exception($msg, 0, $prev ?? null);
        };

        $client = new Client($this->eventLoop, $this->connectionOptions);
        $client->connect()
            ->then(function (Client $client) {
                return $client->channel();
            }, $onReject)
            ->then(function (Channel $channel) use ($onReject) {
                return $channel->qos(0, 1)->then(function () use ($channel) {
                    return $channel;
                }, $onReject);
            }, $onReject)
            ->then(function (Channel $channel) use ($client) {
                $tags = $this->bindConsumer($channel, $this->queues, $this->handler);
                $this->connections[] = [
                    'client' => $client,
                    'channel' => $channel,
                    'tags' => array_keys($tags),
                ];
                $this->queueMap = array_merge($this->queueMap, $tags);
            }, $onReject)
            ->done(function () {}, $onReject);
    }

    /**
     * @param Channel $channel
     * @param array $queues
     * @param callable $handler
     *
     * @return array
     */
    private function bindConsumer(Channel $channel, array $queues, callable $handler): array
    {
        $tags = [];
        foreach ($queues as $queueName) {
            $tag = Helper::uuid();
            $channel->consume($handler, $queueName, $tag)->done();
            $tags[$tag] = $queueName;
        }

        return $tags;
    }

    /**
     * @param Channel $channel
     * @param array $tags
     */
    private function unbindConsumer(Channel $channel, array $tags)
    {
        foreach ($tags as $tag) {
            $channel->cancel($tag)->done();
        }
    }
}