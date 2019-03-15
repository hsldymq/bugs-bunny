<?php

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractMaster;
use Archman\Whisper\Helper;
use Archman\Whisper\Interfaces\WorkerFactoryInterface;
use Archman\Whisper\Message;
use Bunny\Async\Client;
use Bunny\Channel;
use Bunny\Message as AMQPMessage;
use Psr\Log\LoggerInterface;

/**
 * @event message
 * @event processed
 * @event limitReached
 * @event workerQuit
 * @event errorShuttingDown
 * @event errorSendingMessage
 * @event errorMessageHandling
 * @event errorDispatchingMessage
 * @event errorCreateWorker
 */
class Dispatcher extends AbstractMaster
{
    use EventEmitterTrait;

    const STATE_RUNNING = 1;
    const STATE_SHUTTING = 2;
    const STATE_SHUTDOWN = 3;

    /**
     * @var Connection
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
    protected $workersInfo = [];

    /**
     * @var WorkerFactoryInterface
     */
    protected $workerFactory;

    /**
     * @var WorkerScheduler
     */
    protected $workerScheduler;

    /**
     * @var int $maxWorkers 子进程数量上限. -1为不限制
     *
     * 当子进程到达上线后,调度器会停止接受消息队列的消息.
     * 等待有子进程闲置后,继续派发消息.
     */
    protected $maxWorkers = -1;

    /**
     * @var int 每个worker的消息队列容量
     */
    protected $workerCapacity = 1;

    /**
     * @var array
     * [
     *      $pid => $workerID,
     *      ...
     * ]
     */
    protected $idMap = [];

    /**
     * @var array 统计信息.
     * [
     *      'consumed' => (int),            // 消费了的消息总数
     *      'processed' => (int),           // 处理了的消息总数
     *      'peakWorkerNum' => (int),       // worker数量峰值
     * ]
     */
    protected $stat = [
        'consumed' => 0,
        'processed' => 0,
        'peakWorkerNum' => 0,
    ];

    /**
     * @var LoggerInterface|null
     */
    protected $logger = null;

    /**
     * @var int 僵尸进程检查周期(秒)
     */
    protected $patrolPeriod = 300;

    /**
     * @var string 运行状态 self::STATE_RUNNING / self::STATE_SHUTTING / self::SHUTDOWN
     */
    protected $state = self::STATE_SHUTDOWN;

    /**
     * @param Connection $connection
     * @param WorkerFactoryInterface $factory
     */
    public function __construct(Connection $connection, WorkerFactoryInterface $factory)
    {
        parent::__construct();

        $this->workerScheduler = new WorkerScheduler($this->workerCapacity);
        $this->workerFactory = $factory;
        $this->connection = $connection;
        $this->connection->connect($this->getEventLoop(), [$this, 'onConsume']);

        $this->on('__workerExit', function (string $workerID, int $pid) {
            try {
                $this->emit('workerQuit', [$workerID, $pid]);
            } finally {
                $this->clearWorker($workerID, $pid);
                if ($this->state === self::STATE_SHUTTING && $this->countWorkers() === 0) {
                    $this->state = self::STATE_SHUTDOWN;
                    $this->stopProcess();
                }
            }
        });
    }

    public function __destruct()
    {
        $this->connection->disconnect();
    }

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
    }

    public function shutdown()
    {
        $this->connection->disconnect();
        if ($this->countWorkers() === 0) {
            $this->state = self::STATE_SHUTDOWN;
            $this->stopProcess();
            return;
        }

        $this->state = self::STATE_SHUTTING;
        foreach ($this->workersInfo as $workerID => $each) {
            try {
                $this->sendMessage($workerID, new Message(MessageTypeEnum::LAST_MSG, ''));
            } catch (\Throwable $e) {
                // TODO 如果没有成功向所有进程发送关闭,考虑在其他地方需要做重试机制
                if ($this->logger) {
                    $msg = "Failed to send message(to {$workerID}). {$e->getMessage()}.";
                    $this->logger->error($msg, ['trace' => $e->getTrace()]);
                }
                $this->emit('errorShuttingDown', [$e]);
            }
        }
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
                    if (!$this->limitReached()) {
                        $this->connection->resume();
                    }
                    $this->emit('processed', [$workerID]);
                    break;
                case MessageTypeEnum::STOP_SENDING:
                    $this->workerScheduler->retire($workerID);
                    try {
                        $this->sendMessage($workerID, new Message(MessageTypeEnum::LAST_MSG, json_encode([
                            'messageID' => Helper::uuid(),
                            'meta' => ['sent' => $this->workersInfo[$workerID]['sent']],
                        ])));
                    } catch (\Throwable $e) {
                        if ($this->logger) {
                            $this->logger->error($e->getMessage(), ['trace' => $e->getTrace()]);
                        }
                        $this->emit('errorSendingMessage', [$e]);
                    }
                    break;
                case MessageTypeEnum::KILL_ME:
                    $this->killWorker($workerID, SIGKILL, true);
                    break;
                default:
                    $this->emit('message', [$workerID, $message]);
            }
        } catch (\Throwable $e) {
            if ($this->logger) {
                $this->logger->error($e->getMessage(), [
                    'trace' => $e->getTrace(),
                ]);
            }
            $this->emit('errorMessageHandling', [$e]);
        }
    }

    public function onConsume(AMQPMessage $message, Channel $channel, Client $client)
    {
        $reachedBefore = $this->limitReached();
        try {
            $workerID = $this->scheduleWorker();
        } catch (\Throwable $e) {
            return;
        }

        $messageContent = json_encode([
            'messageID' => Helper::uuid(),
            'meta' => [
                'amqp' => [
                    'exchange' => $message->exchange,
                    'queue' => $this->connection->getQueue($message->consumerTag),
                    'routingKey' => $message->routingKey,
                ],
                'sent' => $this->workersInfo[$workerID]['sent'] + 1,
            ],
            'content' => $message->content,
        ]);

        try {
            $this->sendMessage($workerID, new Message(MessageTypeEnum::QUEUE, $messageContent));
        } catch (\Throwable $e) {
            $errorOccurred = true;
            $this->workerScheduler->release($workerID);
            if ($this->logger) {
                $this->logger->error($e->getMessage(), ['trace' => $e->getTrace()]);
            }
            $this->emit('errorDispatchingMessage', [$e]);
        }

        if (!($errorOccurred ?? false)) {
            $this->stat['consumed']++;
            $this->workersInfo[$workerID]['sent']++;
            $channel->ack($message)->done();

            if ($this->limitReached()) {
                $this->connection->pause();
                // 边沿触发
                if (!$reachedBefore) {
                    $this->emit('limitReached');
                }
            }
        }
    }

    /**
     * @param LoggerInterface $logger
     *
     * @return self
     */
    public function setLogger(LoggerInterface $logger): self
    {
        $this->logger = $logger;

        return $this;
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
     * 设置每个worker的消息队列容量.
     *
     * 例如: 当这个值为10,一个worker正在处理一条消息,dispatcher可以继续向它发送9条消息等待它处理.
     * 不建议将这个值设置的.
     *
     * @param int $n 必须大于等于1
     *
     * @return self
     */
    public function setWorkerCapacity(int $n): self
    {
        if ($n > 0) {
            $this->workerCapacity = $n;
            $this->workerScheduler->changeLevels($n);
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
        ];
    }

    /**
     * 是否已到达派发消息上限.
     *
     * 上限是指创建的worker数量已达上限,并且已没有空闲worker可以调度
     *
     * @return bool
     */
    protected function limitReached(): bool
    {
        if ($this->maxWorkers === -1) {
            return false;
        }

        return $this->countWorkers() >= $this->maxWorkers
            && $this->workerScheduler->countSchedulable() === 0;
    }

    /**
     * @param string $workerID
     * @param int $pid
     */
    protected function clearWorker(string $workerID, int $pid)
    {
        $this->workerScheduler->remove($workerID);
        unset($this->workersInfo[$workerID]);
        unset($this->idMap[$pid]);
    }

    /**
     * 安排一个worker,如果没有空闲worker,创建一个.
     *
     * @return string worker id
     * @throws
     */
    protected function scheduleWorker(): string
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
                if ($this->logger) {
                    $this->logger->error($e->getMessage(), ['trace' => $e->getTrace()]);
                }
                $this->emit('errorCreateWorker', [$e]);
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
}