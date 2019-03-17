<?php

namespace Archman\BugsBunny;

use Archman\BugsBunny\Exception\ChannelingFailedException;
use Archman\BugsBunny\Exception\ConnectFailedException;
use Archman\BugsBunny\Exception\ConsumerBindingException;
use Archman\BugsBunny\Exception\NotConnectedException;
use Archman\BugsBunny\Interfaces\AMQPConnectionInterface;
use Archman\BugsBunny\Interfaces\ConsumerHandlerInterface;
use Archman\Whisper\Helper;
use Bunny\Async\Client;
use Bunny\Channel;
use Bunny\Message as AMQPMessage;
use Evenement\EventEmitter;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use function React\Promise\all;
use function React\Promise\any;
use function React\Promise\reject;
use function React\Promise\resolve;

class Connection extends EventEmitter implements AMQPConnectionInterface
{
    const STATE_DISCONNECTED = 'disconnected';
    const STATE_CONNECTED = 'connected';
    const STATE_PAUSING = 'pausing';
    const STATE_PAUSED = 'paused';

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
     * @var Client
     */
    private $client;

    /**
     * @var Channel[]
     */
    private $channels = [];

    /**
     * @var int
     */
    private $numChannels = 10;

    /**
     * @var string
     */
    private $state = self::STATE_DISCONNECTED;

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

    /**
     * @param LoopInterface $eventLoop
     * @param ConsumerHandlerInterface $handler
     *
     * @return PromiseInterface
     */
    public function connect(LoopInterface $eventLoop, ConsumerHandlerInterface $handler): PromiseInterface
    {
        if ($this->state !== self::STATE_DISCONNECTED) {
            return resolve();
        }

        $this->eventLoop = $eventLoop;
        $this->handler = $handler;
        $client = new Client($this->eventLoop, $this->connectionOptions);
        return $client->connect()
            ->then(function (Client $client) {
                $this->state = self::STATE_CONNECTED;
                $this->client = $client;
                return $this->establishChannels();
            }, function ($reason) {
                if ($reason instanceof \Throwable) {
                    return new ConnectFailedException($reason->getMessage(), null, $reason);
                } else {
                    return new ConnectFailedException($reason);
                }
            })
            ->then(function () {
                return $this->bindConsumer($this->channels[0]);
            });
    }

    /**
     * @return PromiseInterface
     */
    public function disconnect(): PromiseInterface
    {
        if ($this->state === self::STATE_DISCONNECTED || !$this->client) {
            return resolve();
        }

        if (!$this->client->isConnected()) {
            $this->state = self::STATE_DISCONNECTED;
            $this->client = null;
            return resolve();
        }

        return $this->client->disconnect()->then(function () {
            $this->state = self::STATE_DISCONNECTED;
            $this->client = null;
        });
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
    public function pause(): PromiseInterface
    {
        if ($this->state !== self::STATE_CONNECTED) {
            return resolve();
        }

        $this->state = self::STATE_PAUSING;
        /** @var Channel $channel */
        $channel = $this->channels[0];
        return $channel->close()->then(function () {
            array_shift($this->channels);
            $this->state = self::STATE_PAUSED;
            $this->emit("resumable");
        }, function ($reason) {
            if ($this->client->isConnected()) {
                $this->state = self::STATE_CONNECTED;
            } else {
                $this->state = self::STATE_DISCONNECTED;
            }
            return $reason;
        });
    }

    /**
     * 恢复消费队列消息.
     */
    public function resume(): PromiseInterface
    {
        if (!$this->state === self::STATE_DISCONNECTED) {
            return reject(new NotConnectedException());
        }

        if ($this->state === self::STATE_CONNECTED) {
            return resolve();
        }

        $promise = resolve();
        if (count($this->channels) === 0) {
            $promise = $this->establishChannels();
        }

        if ($this->state === self::STATE_PAUSING) {
            return resolve();
        }

        return $promise
            ->then(function () {
                return $this->bindConsumer($this->channels[0]);
            })
            ->then(function () {
                $this->state = self::STATE_CONNECTED;
            });
    }

    public function onConsume(AMQPMessage $msg, Channel $channel, Client $client)
    {
        $tag = $msg->consumerTag;
        $queue = $this->queues[$tag];

        $result = $this->handler->onConsume($msg, $queue, $channel, $client);
        if ($result) {
            $channel->ack($msg)->then();
        }
    }

    /**
     * @return PromiseInterface
     */
    private function establishChannels(): PromiseInterface
    {
        $promises = [];
        for ($i = 0; $i < $this->numChannels; $i++) {
            $promises[] = $this->client->channel()->then(function (Channel $channel) {
                $this->channels[] = $channel;
            });
        }

        return any($promises)->then(null, function ($reason) {
            if ($reason instanceof \Throwable) {
                return reject(new ChannelingFailedException($reason->getMessage(), null, $reason));
            } else {
                return reject(new ChannelingFailedException($reason));
            }
        });
    }

    /**
     * @param Channel $channel
     *
     * @return PromiseInterface
     */
    private function bindConsumer(Channel $channel): PromiseInterface
    {
        if (!$this->queues) {
            return resolve();
        }
        $promises = [];
        foreach ($this->queues as $tag => $queue) {
            $promises[] = $channel->consume([$this, 'onConsume'], $queue, $tag);
        }

        return all($promises)->then(null, function ($reason) {
            if ($reason instanceof \Throwable) {
                return reject(new ConsumerBindingException($reason->getMessage(), null, $reason));
            } else {
                return reject(new ConsumerBindingException($reason));
            }
        });
    }
}