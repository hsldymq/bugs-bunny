<?php

namespace Archman\BugsBunny;

use Archman\BugsBunny\Interfaces\AMQPConnectionInterface;
use Archman\BugsBunny\Interfaces\ConsumerHandlerInterface;
use Archman\Whisper\AbstractMaster;
use Archman\Whisper\Helper;
use Archman\Whisper\Interfaces\WorkerFactoryInterface;
use Archman\Whisper\Message;
use Bunny\Async\Client;
use Bunny\Channel;
use Bunny\Message as AMQPMessage;

/**
 * @event message       参数: string $workerID, \Archman\Whisper\Message $msg
 * @event processed     参数: string $workerID, Dispatcher $master
 * @event workerExit    参数: string $workerID, int $pid, Dispatcher $master
 * @event limitReached  参数: Dispatcher $master
 * @event error         参数: string $reason, \Throwable $ex, Dispatcher $master
 *                      $reason enum:
 *                          'shuttingDown'
 *                          'sendingMessage'
 *                          'handlingMessage'
 *                          'dispatchingMessage',
 *                          'creatingWorker'
 */
class Dispatcher extends AbstractMaster implements ConsumerHandlerInterface
{
    use EventEmitterTrait;

    const STATE_RUNNING = 1;
    const STATE_SHUTTING = 2;
    const STATE_SHUTDOWN = 3;

    /**
     * @var AMQPConnectionInterface
     */
    private $connection;

    /**
     * @var array
     * [
     *      $workerID => [
     *          'sent' => (int),        // 已向该worker投放的消息数量
     *          'processed' => (int),   // 已处理的消息数量
     *      ],
     *      ...
     * ]
     */
    private $workersInfo = [];

    /**
     * @var WorkerFactoryInterface
     */
    private $workerFactory;

    /**
     * @var WorkerScheduler
     */
    private $workerScheduler;

    /**
     * @var int $maxWorkers 子进程数量上限. -1为不限制
     *
     * 当子进程到达上线后,调度器会停止接受消息队列的消息.
     * 等待有子进程闲置后,继续派发消息.
     */
    private $maxWorkers = -1;

    /**
     * @var array
     * [
     *      $pid => $workerID,
     *      ...
     * ]
     */
    private $idMap = [];

    /**
     * @var array 统计信息.
     * [
     *      'consumed' => (int),            // 消费了的消息总数
     *      'processed' => (int),           // 处理了的消息总数
     *      'peakWorkerNum' => (int),       // worker数量峰值
     * ]
     */
    private $stat = [
        'consumed' => 0,
        'processed' => 0,
        'peakWorkerNum' => 0,
    ];

    /**
     * @var array 当限制了worker数量并且worker跑满,进来的消息缓存起来,待有空闲的时候再派发
     */
    private $cachedMessages = [];

    /**
     * @var int 限制了缓存消息的数量,如果到达或超过此值时会停止从AMQP消费消息,直到缓存的消息都派发完为止
     */
    private $cacheLimit = 0;

    /**
     * @var int 僵尸进程检查周期(秒)
     */
    private $patrolPeriod = 300;

    /**
     * @var string 运行状态 self::STATE_RUNNING / self::STATE_SHUTTING / self::SHUTDOWN
     */
    private $state = self::STATE_SHUTDOWN;

    /**
     * @var \Throwable
     */
    private $shutdownError;

    public function __construct(AMQPConnectionInterface $connection, WorkerFactoryInterface $factory)
    {
        parent::__construct();

        $this->connection = $connection;
        $this->connection
            ->connect($this->getEventLoop(), $this)
            ->then(function (\Throwable $reason) {
                throw $reason;
            });

        // 不在需要为每个worker设置一个的缓冲队列了
        // 因为有了全局缓冲
        // 对每个worker设置一个缓冲队列有一个问题
        //      如果一个worker队列满了,但是它处理当前消息很慢
        //      那么队列中的消息就会一直等待,但它们其实可以调度给其他已经空闲了的worker在处理
        // 基于以上,这里传参恒为1
        $this->workerScheduler = new WorkerScheduler(1);
        $this->workerFactory = $factory;

        $this->on('__workerExit', function (string $workerID, int $pid) {
            try {
                $this->emit('workerExit', [$workerID, $pid]);
            } finally {
                $this->clearWorker($workerID, $pid);
                if ($this->state === self::STATE_SHUTTING && $this->countWorkers() === 0) {
                    $this->state = self::STATE_SHUTDOWN;
                    $this->stopProcess();
                }
            }
        });

        $this->connection->on('resumable', function () {
            $this->tryDispatchCached();
        });
    }

    /**
     * @throws \Throwable
     */
    public function run()
    {
        if ($this->state !== self::STATE_SHUTDOWN) {
            return;
        }

        $this->workersInfo = [];
        $this->idMap = [];
        $this->state = self::STATE_RUNNING;

        while ($this->state !== self::STATE_SHUTDOWN) {
            $this->process($this->patrolPeriod);
            // 补杀僵尸进程
            for ($i = 0, $len = count($this->idMap); $i < $len; $i++) {
                $pid = pcntl_wait($status, WNOHANG);
                if ($pid <= 0) {
                    break;
                }
                $this->clearWorker($this->idMap[$pid] ?? '', $pid);
            }
        }

        $this->emit('shutdown');

        if ($this->shutdownError) {
            throw $this->shutdownError;
        }
    }

    public function shutdown(\Throwable $withError = null)
    {
        if ($this->state !== self::STATE_RUNNING) {
            return;
        }

        $this->connection->disconnect();

        $this->state = self::STATE_SHUTTING;
        $withError && $this->shutdownError = $withError;
        if (count($this->cachedMessages) > 0) {
            return;
        }

        if ($this->countWorkers() === 0) {
            $this->state = self::STATE_SHUTDOWN;
            $this->stopProcess();
            return;
        }

        $this->informWorkersQuit();
    }

    public function onMessage(string $workerID, Message $message)
    {
        $type = $message->getType();

        try {
            switch ($type) {
                case MessageTypeEnum::PROCESSED:
                    $this->stat['processed']++;
                    if (isset($this->workersInfo[$workerID])) {
                        $this->workersInfo[$workerID]['processed']++;
                    }
                    $this->workerScheduler->release($workerID);
                    $this->tryDispatchCached();
                    $this->emit('processed', [$workerID]);
                    break;
                case MessageTypeEnum::STOP_SENDING:
                    $this->workerScheduler->retire($workerID);
                    try {
                        $this->sendLastMessage($workerID);
                    } catch (\Throwable $e) {
                        $this->emit('error', ['sendingMessage', $e]);
                    }
                    break;
                case MessageTypeEnum::KILL_ME:
                    $this->killWorker($workerID, SIGKILL);
                    break;
                default:
                    $this->emit('message', [$workerID, $message]);
            }
        } catch (\Throwable $e) {
            $this->emit('error', ['handlingMessage', $e]);
        }
    }

    public function onConsume(
        AMQPMessage $AMQPMessage,
        string $queue,
        Channel $channel,
        Client $client
    ): bool {
        $reachedBefore = $this->limitReached();
        $message = [
            'messageID' => Helper::uuid(),
            'meta' => [
                'amqp' => [
                    'exchange' => $AMQPMessage->exchange,
                    'queue' => $queue,
                    'routingKey' => $AMQPMessage->routingKey,
                ],
                'sent' => -1,       // 在dispatch的时候会填上准确的值,它代表已经向该worker派发了多少条队列消息
                                    // 用于worker退出阶段处理掉剩余的消息
            ],
            'content' => $AMQPMessage->content,
        ];

        if ($reachedBefore) {
            $this->cachedMessages[] = $message;
        } else {
            try {
                $this->dispatch($message);
            } catch (\Throwable $e) {
                $this->emit('error', ['dispatchingMessage', $e]);
                return false;
            }
        }

        try {
            $channel->ack($AMQPMessage)->then();
            if ($this->limitReached()) {
                // 无空闲worker且缓存消息已满
                if (count($this->cachedMessages) >= $this->cacheLimit) {
                    $this->connection->pause()->then(null, function ($err) {
                        $this->shutdown($err);
                    });
                }

                // 边沿触发
                if (!$reachedBefore) {
                    $this->emit('limitReached');
                }
            }
        } finally {
            return true;
        }
    }

    /**
     * 设置进程数量上限.
     *
     * @param int $num >0时为上限值, =-1时为不限数量, 0或者<-1时不做任何操作.
     *
     * @return self
     */
    public function setMaxWorkers(int $num): self
    {
        if ($num > 0 || $num === -1) {
            $this->maxWorkers = $num;
        }

        return $this;
    }

    /**
     * 设置缓存消息的数量上限.
     *
     * 当限制了worker上限并且worker跑满没有空闲时,到来消息会被缓存起来
     * 待有空闲worker时优先从缓存中取出小心进行派发
     * 这个方法限制了缓存的大小,当缓存到达上限后,会暂停从amqp server中消费消息
     * 直到清空了缓存为止
     * 建议不要设置太大,因为可能会大量消耗master进程的内存资源,不利于进程的稳定常驻运行.
     *
     * @param int $limit >= 0
     */
    public function setCacheLimit(int $limit)
    {
        if ($limit < 0) {
            return;
        }

        $this->cacheLimit = $limit;
    }

    /**
     * 进行僵尸进程检查的周期.
     *
     * 例如设置为5分钟,那么每5分钟会调用pcntl_wait清理掉没有成功清除的僵尸进程
     *
     * @param int $seconds
     *
     * @return Dispatcher
     */
    public function setPatrolPeriod(int $seconds): self
    {
        if ($seconds > 0) {
            $this->patrolPeriod = $seconds;
        }

        return $this;
    }

    /**
     * @return array
     */
    public function getStat(): array
    {
        return $this->stat;
    }

    public function clearStat()
    {
        $this->stat = [
            'consumed' => 0,
            'processed' => 0,
        ];
    }

    /**
     * 返回还可以调度的进程数量
     *
     * @return int
     */
    public function countSchedulable(): int
    {
        return $this->workerScheduler->countSchedulable();
    }

    /**
     * 是否已到达派发消息上限.
     *
     * 上限是指创建的worker数量已达上限,并且已没有空闲worker可以调度
     *
     * @return bool
     */
    public function limitReached(): bool
    {
        if ($this->maxWorkers === -1) {
            return false;
        }

        return $this->countWorkers() >= $this->maxWorkers
            && $this->workerScheduler->countSchedulable() === 0;
    }

    /**
     * 派发消息.
     *
     * @param array $message
     *
     * @return string 派发给的worker id
     * @throws
     */
    private function dispatch(array $message): string
    {
        $workerID = $this->scheduleWorker();

        $message['meta']['sent'] = $this->workersInfo[$workerID]['sent'] + 1;
        try {
            $this->sendMessage($workerID, new Message(MessageTypeEnum::QUEUE, json_encode($message)));
        } catch (\Throwable $e) {
            $this->workerScheduler->release($workerID);
            throw $e;
        }
        $this->stat['consumed']++;
        $this->workersInfo[$workerID]['sent']++;

        return $workerID;
    }

    /**
     * @param string $workerID
     * @param int $pid
     */
    private function clearWorker(string $workerID, int $pid)
    {
        $this->workerScheduler->remove($workerID);
        unset($this->workersInfo[$workerID]);
        unset($this->idMap[$pid]);
    }

    private function tryDispatchCached()
    {
        if (!$this->limitReached()) {
            // 优先派发缓存的消息
            if (count($this->cachedMessages) > 0) {
                $msg = array_shift($this->cachedMessages);
                try {
                    $this->dispatch($msg);
                } catch (\Throwable $e) {
                    array_unshift($this->cachedMessages, $msg);
                    $this->emit('error', ['dispatchingMessage', $e]);
                }
            } else {
                $this->connection->resume()
                    ->then(null, function ($err) {
                        $this->shutdown($err);
                    });
            }
        }
    }

    /**
     * 安排一个worker,如果没有空闲worker,创建一个.
     *
     * @return string worker id
     * @throws
     */
    private function scheduleWorker(): string
    {
        while (($workerID = $this->workerScheduler->allocate()) !== null) {
            $c = $this->getCommunicator($workerID);
            if ($c && $c->isWritable()) {
                break;
            }
        }

        if (!$workerID) {
            try {
                $workerID = $this->createWorker($this->workerFactory);
            } catch (\Throwable $e) {
                $this->emit('error', ['creatingWorker', $e]);
                throw $e;
            }
            $this->stat['peakWorkerNum'] = max($this->countWorkers(), $this->stat['peakWorkerNum'] + 1);

            $pid = $this->getWorkerPID($workerID);
            $this->workerScheduler->add($workerID, true);
            $this->idMap[$pid] = $workerID;
            $this->workersInfo[$workerID] = [
                'sent' => 0,
                'processed' => 0,
            ];
        }

        return $workerID;
    }

    private function sendLastMessage(string $workerID)
    {
        $this->sendMessage($workerID, new Message(MessageTypeEnum::LAST_MSG, json_encode([
            'messageID' => Helper::uuid(),
            'meta' => ['sent' => $this->workersInfo[$workerID]['sent']],
        ])));
    }

    /**
     * 通知worker退出.
     */
    private function informWorkersQuit()
    {
        foreach ($this->workersInfo as $workerID => $each) {
            try {
                $this->sendLastMessage($workerID);
            } catch (\Throwable $e) {
                // TODO 如果没有成功向所有进程发送关闭,考虑在其他地方需要做重试机制
                $this->emit('error', ['shuttingDown', $e]);
            }
        }
    }
}