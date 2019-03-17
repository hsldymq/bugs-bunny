<?php

namespace Archman\BugsBunny;

use Archman\BugsBunny\Interfaces\AMQPConnectionInterface;
use Archman\BugsBunny\Interfaces\ConsumerHandlerInterface;
use Archman\Whisper\Helper;
use Bunny\Async\Client;
use Bunny\Channel;
use Bunny\Message as AMQPMessage;
use React\EventLoop\LoopInterface;

class NewConnection implements AMQPConnectionInterface
{
    /**
     * @var LoopInterface
     */
    private $eventLoop;

    /**
     * @var ConsumerHandlerInterface
     */
    private $handler;

    /**
     * @var array
     * [
     *      $consumerTag => $queueName,
     *      ...
     * ]
     */
    private $queues = [];

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
     * @var array
     * [
     *      'client' => $client,
     *      'channels' => [
     *          $channel,
     *          ...
     *      ],
     *      'closedClients = [
     *          $client,
     *          ...
     *      ],
     *      'closedChannels => [
     *          $channel,
     *          ...
     *      ],
     * ]
     */
    private $connections = [
        'client' => null,
        'channels' => [],
        'closedClients' => [],
        'closedChannels' => [],
    ];

    /**
     * @var array
     * [
     *      $consumerTag => $queue,
     *      ...
     * ]
     */
    private $queueMap = [];

    /**
     * @var int
     */
    private $openChannelNum = 10;

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
        $this->connectionOptions = array_intersect_key($options, [
            'host' => true,
            'port' => true,
            'vhost' => true,
            'user' => true,
            'password' => true,
        ]);

        $queues = array_unique($queues);
        foreach ($queues as $queueName) {
            $consumerTag = Helper::uuid();
            $this->queues[$consumerTag] = $queueName;
        }
    }

    public function connect(LoopInterface $eventLoop, ConsumerHandlerInterface $handler)
    {
        if ($this->connected) {
            return;
        }

        $this->eventLoop = $eventLoop;
        $this->handler = $handler;
        $this->makeConnection();
        $this->connected = true;
    }

    public function disconnect()
    {
        if (!$this->connected) {
            return;
        }

        /** @var Client $client */
        $client = $this->connections['client'];
        if (!$client) {
            return;
        }
        $this->connections['closedClients'][] = $client;
        $client->disconnect()->done((function (int $index) {
            return function () use ($index) {
                $this->connections['client'] = null;
                unset($this->connections['closedClients'][$index]);
            };
        })(count($this->connections['closedClients']) - 1));
        $this->connected = false;
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
        if (!$this->connected || $this->paused) {
            return;
        }

        /** @var Channel $channel */
        $channel = array_shift($this->connections['channels']);
        if (!$channel) {
            return;
        }

        $this->connections['closedChannels'][] = $channel;
        $this->batchUnbindConsumers($channel, array_keys($this->queues), (function (int $index) {
            return function () use ($index) {
                unset($this->connections['closedChannels']);
            };
        })(count($this->connections['closedChannels']) - 1));
        $this->paused = true;
    }

    /**
     * 恢复消费队列消息.
     */
    public function resume()
    {
        if (!$this->connected || !$this->paused) {
            return;
        }

        foreach ($this->connections as $index => $each) {
            foreach ($each['tags'] as $tag) {
                $queue = $this->queueMap[$tag];
                $this->bindConsumer($each['channel'], $tag, $queue, $this->handler);
            }
        }
        $this->paused = false;
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * @return bool
     */
    public function isPaused(): bool
    {
        return $this->paused;
    }

    public function onConsume(AMQPMessage $msg, Channel $channel, Client $client)
    {
        $tag = $msg->consumerTag;
    }

    /**
     * @return array [
     *      'client' => $client,
     *      'channels' => [
     *          $channel,
     *          ...
     *      ],
     * ]
     */
    private function makeConnection(): array
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
        for ($i = 0; $i < $this->openChannelNum; $i++) {
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
                    $map = [];
                    foreach ($this->queues as $queue) {
                        $tag = Helper::uuid();
                        $this->bindConsumer($channel, $tag, $queue, $this->handler);
                        $map[$tag] = $queue;
                    }
                    $this->connections[] = [
                        'client' => $client,
                        'channel' => $channel,
                        'tags' => array_keys($map),
                    ];
                    $this->queueMap = array_merge($this->queueMap, $map);
                }, $onReject)
                ->done(function () {}, $onReject);
        }



    }

    /**
     * @param Channel $channel
     * @param string $tag
     * @param string $queue
     * @param callable $handler
     */
    private function bindConsumer(Channel $channel, string $tag, string $queue, callable $handler)
    {
        $channel->consume($handler, $queue, $tag)->done();
    }

    private function batchUnbindConsumers(Channel $channel, array $tags, callable $afterUnbind)
    {
        $onFulfilled = (function (int $tagNum, callable $afterUnbind) {
            $unbindNum = 0;
            return function () use (&$unbindNum, $tagNum, $afterUnbind) {
                $unbindNum++;
                if ($unbindNum >= $tagNum) {
                    $afterUnbind();
                }
            };
        })(count($tags), $afterUnbind);

        foreach ($tags as $eachTag) {
            $channel->cancel($eachTag)->then($onFulfilled)->done();
        }
    }

    /**
     * @param Channel $channel
     * @param string $tag
     */
    private function unbindConsumer(Channel $channel, string $tag)
    {
        $channel->cancel($tag)->done();
    }
}