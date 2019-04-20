<?php

declare(strict_types=1);

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
use React\Promise\Promise;

/**
 * 可以使用on方法监听以下预定义事件:
 * @event start         dispatcher启动
 *
 * @event patrolling    进行一次僵尸进程检查周期
 *
 * @event processed     一条amqp消息被worker成功处理
 *                      参数: string $workerID, Dispatcher $master
 *
 * @event workerExit    worker退出
 *                      参数: string $workerID, int $pid, Dispatcher $master
 *
 * @event limitReached  worker数量达到上限
 *                      参数: Dispatcher $master
 *
 * @event message       worker发来一条自定义消息
 *                      参数: string $workerID, \Archman\Whisper\Message $msg, Dispatcher $master
 *
 * @event error         出现错误
 *                      参数: string $reason, \Throwable $ex, Dispatcher $master
 *                      $reason enum:
 *                          'shuttingDown'          程序结束,关闭剩余worker过程中出错
 *                          'dispatchingMessage'    向worker投放amqp消息时出错
 *                          'sendingMessage'        向worker发送控制信息时出错
 *                          'creatingWorker'        创建worker时出错
 */
class Dispatcher extends AbstractMaster implements ConsumerHandlerInterface
{
    use EventEmitterTrait;

    const STATE_RUNNING = 1;
    const STATE_FLUSHING = 2;
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
     *      'maxMessageLength' => (int),    // 最大消息大小(字节)
     *      'peakNumWorkers' => (int),      // worker数量峰值
     *      'peakNumCached' => (int),       // 缓存消息数量峰值
     * ]
     */
    private $stat = [
        'consumed' => 0,
        'processed' => 0,
        'maxMessageLength' => 0,
        'peakNumWorkers' => 0,
        'peakNumCached' => 0,
    ];

    /**
     * @var \SplDoublyLinkedList 当限制了worker数量并且worker跑满,进来的消息缓存起来,待有空闲的时候再派发
     */
    private $cachedMessages;

    /**
     * @var int 限制了缓存消息的数量,如果到达或超过此值时会停止从AMQP消费消息,直到缓存的消息都派发完为止
     */
    private $cacheLimit = 0;

    /**
     * @var int 僵尸进程检查周期(秒)
     */
    private $patrolPeriod = 300;

    /**
     * @var string 运行状态 self::STATE_RUNNING / self::STATE_FLUSHING / self::SHUTDOWN
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
        $this->workerFactory = $factory;
        $this->cachedMessages = new \SplDoublyLinkedList();
        $this->workerScheduler = new WorkerScheduler();

        $this->on('__workerExit', function (string $workerID, int $pid) {
            $this->errorlessEmit('workerExit', [$workerID, $pid]);
            $this->clearWorker($workerID, $pid);
        });
    }

    /**
     * @param bool $daemonize
     *
     * @throws \Throwable
     */
    public function run(bool $daemonize = false)
    {
        if ($this->state !== self::STATE_SHUTDOWN) {
            return;
        }

        if ($daemonize) {
            $this->daemonize();
        }

        $this->errorlessEmit('start');

        $this->workersInfo = [];
        $this->idMap = [];
        $this->state = self::STATE_RUNNING;

        $this->connection
            ->connect($this->getEventLoop(), $this)
            ->then(null, function (\Throwable $reason) {
                $this->shutdown($reason);
            });

        while ($this->state === self::STATE_RUNNING) {
            try {
                $this->process($this->patrolPeriod);
            } catch (\Throwable $e) {
                $this->shutdown($e);
            }

            $this->errorlessEmit('patrolling');

            // 补杀僵尸进程
            for ($i = 0, $len = count($this->idMap); $i < $len; $i++) {
                $pid = pcntl_wait($status, WNOHANG);
                if ($pid <= 0) {
                    break;
                }
                $this->clearWorker($this->idMap[$pid] ?? '', $pid);
            }
        }

        // 将剩余的缓存消息都处理完
        $this->flushCached();

        // 使所有worker都退出
        $this->informWorkersQuit();

        $this->errorlessEmit('shutdown');

        if ($this->shutdownError) {
            throw $this->shutdownError;
        }
    }

    /**
     * @param \Throwable|null $withError
     */
    public function shutdown(\Throwable $withError = null)
    {
        if ($this->state !== self::STATE_RUNNING) {
            return;
        }

        if ($withError && !$this->shutdownError) {
            $this->shutdownError = $withError;
        }

        $this->connection->disconnect()->always(function () {
            $this->state = self::STATE_FLUSHING;
            $this->stopProcess();
        });
    }

    /**
     * @param string $workerID
     * @param Message $message
     *
     * @throws
     */
    public function onMessage(string $workerID, Message $message)
    {
        $type = $message->getType();

        switch ($type) {
            case MessageTypeEnum::PROCESSED:
                $this->stat['processed']++;
                $this->workerScheduler->release($workerID);
                if (isset($this->workersInfo[$workerID])) {
                    $this->workersInfo[$workerID]['processed']++;
                }

                $this->errorlessEmit('processed', [$workerID]);
                $this->tryDispatchCached();
                break;
            case MessageTypeEnum::STOP_SENDING:
                // worker主动告知不再希望收到更多队列消息
                // 这时会启动worker关闭沟通流程
                $this->workerScheduler->retire($workerID);
                try {
                    $this->sendLastMessage($workerID);
                } catch (\Throwable $e) {
                    $this->errorlessEmit('error', ['sendingMessage', $e]);
                }
                break;
            case MessageTypeEnum::I_AM_QUIT:
                // 正常的worker关闭流程总是以dispatcher发送一个类型为LAST_MSG的消息开始
                // worker关闭有主动和被动关闭两种模式
                // 对于主动关闭模式,worker收到LAST_MSG,会返回I_AM_QUIT消息通知即将退出
                // dispatcher收到会发送ROGER_THAT告知已准备好worker的退出
                $this->sendMessage($workerID, new Message(MessageTypeEnum::ROGER_THAT, ''));
                break;
            case MessageTypeEnum::KILL_ME:
                // 对于被动关闭模式,worker收到LAST_MSG,会返回KILL_ME消息让dispatcher杀死自己
                // 存在这种模式是因为对于一些扩展(例如grpc 1.20以下的版本),fork出的子进程无法正常退出,只有通过信号来杀死
                // 收到KILL_ME消息代表了worker已经做完了所有工作,所以杀死进程是安全的.
                $this->killWorker($workerID, SIGKILL);
                break;
            default:
                $this->errorlessEmit('message', [$workerID, $message]);
        }
    }

    public function onConsume(
        AMQPMessage $AMQPMessage,
        string $queue,
        Channel $channel,
        Client $client
    ) {
        $reachedBefore = $this->limitReached();
        $this->stat['maxMessageLength'] = max(strlen($AMQPMessage->content), $this->stat['maxMessageLength']);
        $message = [
            'messageID' => Helper::uuid(),
            'meta' => [
                'amqp' => [
                    'exchange' => $AMQPMessage->exchange,
                    'queue' => $queue,
                    'routingKey' => $AMQPMessage->routingKey,
                    'headers' => $AMQPMessage->headers,
                ],
            ],
            'content' => $AMQPMessage->content,
        ];

        if ($reachedBefore) {
            $this->cachedMessages->push($message);
            $this->stat['peakNumCached'] = max($this->stat['peakNumCached'], count($this->cachedMessages));
        } else {
            try {
                $this->dispatch($message);
            } catch (\Throwable $e) {
                $this->errorlessEmit('error', ['dispatchingMessage', $e]);
            }
        }

        /** @var Promise $promise */
        $promise = $channel->ack($AMQPMessage);
        if ($this->limitReached()) {
            if (!$reachedBefore) {
                // 边沿触发
                $this->errorlessEmit('limitReached');
            }

            $promise->always(function () {
                if ($this->state === self::STATE_SHUTDOWN) {
                    return;
                }
                if (count($this->cachedMessages) >= $this->cacheLimit) {
                    $this->connection->pause()->then(null, function (\Throwable $error) {
                        $this->shutdown($error);
                    });
                }
            });
        }
    }

    /**
     * 设置进程数量上限.
     *
     * @param int $num > 0时为上限值, =-1时为不限数量, 0或者<-1时不做任何操作.
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
     * 建议在内存允许的情况下可以适当设大一些.
     *
     * @param int $limit >= 0
     *
     * @return self
     */
    public function setCacheLimit(int $limit): self
    {
        if ($limit >= 0) {
            $this->cacheLimit = $limit;
        }

        return $this;
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
            'maxMessageLength' => 0,
            'peakNumWorkers' => 0,
            'peakNumCached' => 0,
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
     * @param string $event
     * @param array $args
     */
    public function errorlessEmit(string $event, array $args = [])
    {
        try {
            $this->emit($event, $args);
        } finally {}
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
        if ($this->limitReached()) {
            return;
        }

        // 优先派发缓存的消息
        if (count($this->cachedMessages) > 0) {
            try {
                $this->dispatch($this->cachedMessages->offsetGet(0));
                $this->cachedMessages->shift();
            } catch (\Throwable $e) {
                $this->errorlessEmit('error', ['dispatchingMessage', $e]);
            }
        } else if ($this->state === self::STATE_RUNNING) {
            $this->connection->resume()
                ->then(null, function (\Throwable $error) {
                    $this->shutdown($error);
                });
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
                $this->errorlessEmit('error', ['creatingWorker', $e]);
                throw $e;
            }
            $this->stat['peakNumWorkers'] = max($this->countWorkers(), $this->stat['peakNumWorkers'] + 1);

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

    /**
     * @param string $workerID
     *
     * @throws
     */
    private function sendLastMessage(string $workerID)
    {
        $this->sendMessage($workerID, new Message(MessageTypeEnum::LAST_MSG, ''));
    }

    /**
     * 处理掉剩余的缓存消息.
     *
     * @throws
     */
    private function flushCached()
    {
        while ($this->state === self::STATE_FLUSHING && count($this->cachedMessages) > 0) {
            while (!$this->limitReached()) {
                $this->tryDispatchCached();
            }

            $this->process(0.1);
        }
    }

    /**
     * 通知并确保所有worker退出.
     *
     * @throws
     */
    private function informWorkersQuit()
    {
        foreach ($this->workersInfo as $workerID => $each) {
            try {
                $this->sendLastMessage($workerID);
            } catch (\Throwable $e) {
                // TODO 如果没有成功向所有进程发送关闭,考虑是否需要做重试机制
                $this->errorlessEmit('error', ['shuttingDown', $e]);
            }
        }

        do {
            if ($this->countWorkers() === 0) {
                $this->state = self::STATE_SHUTDOWN;
                break;
            }

            $this->process(0.1);
            while (($pid = pcntl_wait($status, WNOHANG)) > 0) {
                $this->clearWorker($this->idMap[$pid] ?? '', $pid);
            }
        } while (true);
    }
}