<?php

namespace Archman\BugsBunny;

use Archman\BugsBunny\Exception\ConnectFailedException;
use Archman\BugsBunny\Exception\ConsumerBindingException;
use Archman\BugsBunny\Exception\NotConnectedException;
use Archman\BugsBunny\Interfaces\AMQPConnectionInterface;
use Archman\BugsBunny\Interfaces\ConsumerHandlerInterface;
use Bunny\Async\Client;
use Bunny\Channel;
use Bunny\Message as AMQPMessage;
use Evenement\EventEmitter;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use function React\Promise\all;
use function React\Promise\reject;
use function React\Promise\resolve;

class Connection extends EventEmitter implements AMQPConnectionInterface
{
    const STATE_DISCONNECTED = 'disconnected';
    const STATE_CONNECTED = 'connected';
    const STATE_PAUSING = 'pausing';
    const STATE_PAUSED = 'paused';
    const STATE_RESUMING = 'resuming';

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
     *      $queueName => $consumerHandler,
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
     * @var Channel
     */
    private $channel;

    /**
     * @var int channel qos的预取消息数
     */
    private $numPrefetch = 1;

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

        foreach (array_unique($queues) as $queueName) {
            $this->queues[$queueName] = (function (string $queueName) {
                return function (AMQPMessage $msg, Channel $channel, Client $client) use ($queueName) {
                    $this->handler->onConsume($msg, $queueName, $channel, $client);
                };
            })($queueName);
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
                return $this->client->channel();
            }, function ($reason) {
                if ($reason instanceof \Throwable) {
                    return new ConnectFailedException($reason->getMessage(), null, $reason);
                } else {
                    return new ConnectFailedException($reason);
                }
            })
            ->then(function (Channel $channel) {
                return $channel->qos(0, $this->numPrefetch)->then(function () use ($channel) {
                    return $channel;
                });
            })
            ->then(function (Channel $channel) {
                $this->channel = $channel;
                return $this->bindConsumer($this->channel);
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

    /**
     * 暂停消费队列消息.
     */
    public function pause(): PromiseInterface
    {
        if ($this->state !== self::STATE_CONNECTED) {
            return resolve();
        }

        $this->state = self::STATE_PAUSING;

        return $this->channel->close()
            ->then(function () {
                $this->state = self::STATE_PAUSED;
                $this->emit("resumable");
                $this->channel = null;
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

        if ($this->state === self::STATE_CONNECTED ||
            $this->state === self::STATE_PAUSING ||
            $this->state === self::STATE_RESUMING
        ) {
            return resolve();
        }

        $this->state = self::STATE_RESUMING;

        return $this->client->channel()
            ->then(function (Channel $channel) {
                return $channel->qos(0, $this->numPrefetch)->then(function () use ($channel) {
                    return $channel;
                });
            })
            ->then(function (Channel $channel) {
                $this->channel = $channel;
                return $this->bindConsumer($this->channel);
            })
            ->then(function () {
                $this->state = self::STATE_CONNECTED;
            });
    }

    /**
     * 请在初始化阶段设置,否则不能即时生效.
     *
     * @param int $num
     */
    public function setPrefetch(int $num)
    {
        if ($num > 0) {
            $this->numPrefetch = $num;
        }
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
        foreach ($this->queues as $queue => $handler) {
            $promises[] = $channel->consume($handler, $queue);
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