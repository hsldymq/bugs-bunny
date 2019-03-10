<?php

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractMaster;
use Archman\Whisper\Helper;
use Archman\Whisper\Interfaces\WorkerFactoryInterface;
use Archman\Whisper\Message;
use Bunny\Async\Client;
use Bunny\Channel;
use Psr\Log\LoggerInterface;

class Dispatcher extends AbstractMaster
{
    /**
     * @var array
     */
    private $connectionOptions;

    /**
     * @var array
     * [
     *      'connections' => [
     *          [
     *              'client' => $client,
     *              'channel' => $channel,
     *              'tags' => [$tag1, $tag2, ...],
     *          ],
     *          ...
     *      ],
     *      'tags' => [
     *          $tag => $queueName,
     *          ...
     *      ]
     * ]
     */
    private $connectionInfo = [
        'connections' => [],
        'tags' => [],
    ];

    /**
     * @var array
     * [
     *      $workerID => [
     *          'retired' => (bool),        // 是否停止向该processor投放队列消息
     *          'sentMessages' => (int),    // 已向该processor投放的消息数量
     *          'level' => (int),           // 投放消息的优先级
     *      ],
     *      ...
     * ]
     */
    private $processorsInfo = [];

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
     *      'consumed' => (int),        // 消费了的消息总数
     *      'processed' => (int),       // 消费了的消息总数
     *      'memoryUsage' => (int),     // 当前内存量,不含未使用的页(字节)
     *      'memoryPeakUsage' => (int)  // 内存使用峰值,不含未使用的页(字节)
     * ]
     */
    private $stat = [
        'consumed' => 0,
        'processed' => 0,
    ];

    /**
     * @var LoggerInterface|null
     */
    private $logger = null;

    /**
     * @var int 僵尸进程检查周期(秒)
     */
    private $patrolPeriod = 300;

    /**
     * @var WorkerFactoryInterface
     */
    private $processorFactory;

    /**
     * @var int $maxProcessors 子进程数量上限. -1为不限制
     *
     * 当子进程到达上线后,调度器会停止接受消息队列的消息.
     * 等待有子进程闲置后,继续派发消息.
     */
    private $maxProcessors = -1;

    /**
     * @var int 每个processor的消息队列容量
     */
    private $processorCapacity = 1;

    /**
     * @var string 运行状态 running / shutting / shutdown.
     */
    private $state = 'shutdown';

    /**
     * @param array $connectionOptions amqp server连接配置
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
     * @param WorkerFactoryInterface $factory
     */
    public function __construct(array $connectionOptions, WorkerFactoryInterface $factory)
    {
        parent::__construct();

        $this->processorFactory = $factory;
        $this->connectionOptions = $connectionOptions;

        $this->on('workerExit', function (string $workerID) {
            $pid = $this->getWorkerPID($workerID);
            $this->clearWorker($workerID, $pid ?? 0);
        });

        $this->addSignalHandler(SIGCHLD, function () {
            $this->tryWaitAndClear();
        });
    }

    public function run()
    {
        if ($this->state !== 'shutdown') {
            return;
        }

        $this->processorsInfo = [];
        $this->idMap = [];

        $this->state = 'running';
        while ($this->state !== 'shutdown') {
            $this->process($this->patrolPeriod);

            // 补杀僵尸进程
            for ($i = 0, $len = count($this->idMap); $i < $len; $i++) {
                if (!$this->tryWaitAndClear()) {
                    break;
                }
            }
        }

        $this->disconnect();
    }

    /**
     * @param array $queues 要拉取消息的队列名称列表
     */
    public function connect(array $queues)
    {
        $connections = $this->connectionOptions['connections'] ?? 1;
        $options = array_intersect_assoc($this->connectionOptions, [
            'host' => true,
            'port' => true,
            'vhost' => true,
            'user' => true,
            'password' => true,
        ]);
        for ($i = 0; $i < $connections; $i++) {
            $this->makeConnection($options, $queues);
        }
    }

    public function disconnect()
    {
        foreach ($this->connectionInfo['connections'] as $index => $each) {
            /** @var Client $client */
            $client = $each['client'];
            $tags = $each['tags'];
            $client->disconnect()->done();
            unset($this->connectionInfo['connections'][$index]);
            foreach ($tags as $t) {
                unset($this->connectionInfo['tags'][$t]);
            }
        }
    }

    /**
     * @param array $queues
     */
    public function reconnect(array $queues)
    {
        $this->disconnect();
        $this->connect($queues);
    }

    public function shutdown()
    {
        $this->state = 'shutting';
        $this->disconnect();

        // TODO 关闭rabbitmq 连接
        // TODO 遍历发送LAST_MSG
        foreach ($this->processorInfo as $each) {

        }
    }

    public function onMessage(string $workerID, Message $msg)
    {
        $type = $msg->getType();

        try {
            switch ($type) {
                case MessageTypeEnum::PROCESSED:
                    break;
                case MessageTypeEnum::STOP_SENDING:
                    break;
                case MessageTypeEnum::KILL_ME:
                    if ($this->killWorker($workerID, SIGKILL, true)) {
                        $this->clearWorker($workerID, $this->getWorkerPID($workerID) ?? 0);
                    }

                    if ($this->state === 'shutting' && count($this->processorsInfo) === 0) {
                        $this->stopProcess();
                        $this->state = 'shutdown';
                    }
                    break;
                default:
                    $this->emit('onMessage', [$workerID, $msg, $this]);
            }
        } catch (\Throwable $e) {
            if ($this->logger) {
                $this->logger->error($e->getMessage(), [
                    'errorTrace' => $e->getTrace(),
                ]);
            }
            $this->emit('onMessageHandleError', [$e]);
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
    public function setMaxProcessors(int $num): self
    {
        if ($num > 0 || $num === -1) {
            $this->maxProcessors = $num;
        }

        return $this;
    }

    /**
     * 设置每个processor的消息队列容量.
     *
     * 例如: 当这个值为10,一个processor正在处理一条消息,dispatcher可以继续向它发送9条消息等待它处理.
     * 不建议将这个值设置的.
     *
     * @param int $n 必须大于等于1
     *
     * @return self
     */
    public function setProcessorCapacity(int $n): self
    {
        if ($n > 0) {
            $this->processorCapacity = $n;
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
        $stat = $this->stat;
        $stat['memoryUsage'] = memory_get_usage();
        $stat['memoryPeakUsage'] = memory_get_peak_usage();

        return $stat;
    }

    public function clearStat()
    {
        $this->stat = [
            'consumed' => 0,
            'processed' => 0,
        ];
    }

    protected function onConsumed()
    {

    }

    protected function getState(): string
    {
        return $this->state;
    }

    /**
     * 是否已到达processor上限.
     *
     * @return bool
     */
    protected function limitReached(): bool
    {
        if ($this->maxProcessors === -1) {
            return false;
        }

        return count($this->processorsInfo) >= $this->maxProcessors;
    }

    /**
     * @return bool
     */
    protected function tryWaitAndClear(): bool
    {
        $pid = pcntl_wait($status, WNOHANG);
        if ($pid > 0) {
            $this->clearWorker($this->idMap[$pid] ?? '', $pid);

            return true;
        }

        return false;
    }

    protected function clearWorker(string $workerID, int $pid)
    {
        if (isset($this->processorsInfo[$workerID])) {
            $level = $this->processorsInfo[$workerID]['level'];
            unset($this->processorsInfo[$workerID]);
            unset($this->sendingPriority[$level][$workerID]);
        }

        unset($this->idMap[$pid]);
    }

    protected function makeConnection(array $options, array $queues)
    {
        $onReject = function ($reason) {
            $msg = "Failed to connect AMQP server.";

            if (is_string($reason)) {
                $msg .= $reason;
            } else if ($reason instanceof \Throwable) {
                $prev = $reason;
                $context = ['trace' => $prev->getTrace()];
                $msg .= $reason->getMessage();
            }

            if ($this->logger) {
                $this->logger->error($msg, $context ?? null);
            }

            throw new \Exception($msg, 0, $prev ?? null);
        };

        $chan = null;
        $tags = [];
        $client = new Client($this->getEventLoop(), $options);
        $client->connect()
            ->then(function (Client $client) {
                return $client->channel();
            }, $onReject)
            ->then(function (Channel $channel) {
                return $channel->qos(0, 1)->then(function () use ($channel) {
                    return $channel;
                });
            }, $onReject)
            ->then(function (Channel $channel) use (&$chan, &$tags, $queues) {
                $chan = $channel;
                foreach ($queues as $queueName) {
                    $tag = Helper::uuid();
                    $channel->consume([$this, 'onConsume'], $queueName, $tag)->done();
                    $tags[$tag] = $queueName;
                }
            }, $onReject)
            ->done();

        $this->connectionInfo['connections'][] = [
            'client' => $client,
            'channel' => $chan,
            'tags' => array_keys($tags),
        ];
        $this->connectionInfo['tags'] = array_merge($this->connectionInfo['tags'], $tags);
    }
}