<?php

declare(strict_types=1);

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractWorker;
use Archman\Whisper\Message;
use React\EventLoop\TimerInterface;

/**
 * 可以使用on方法或在WorkerFactory使用registerEvent方法监听以下预定义事件:
 * @event start             worker启动
 *                          参数: \Archman\BugsBunny\Worker $worker
 *
 * @event patrolling        进行一次巡逻,给使用者定时进行抽样的机会
 *                          参数: \Archman\BugsBunny\Worker $worker
 *
 * @event message           dispatcher发来自定义消息时
 *                          参数: \Archman\Whisper\Message $msg, \Archman\BugsBunny\Worker $worker
 *
 * @event disconnected      与dispatcher的连接中断,即将退出
 *                          参数: \Archman\BugsBunny\Worker $worker
 *
 * @event error             发生错误
 *                          参数: string $reason, \Throwable $ex, \Archman\BugsBunny\Worker $worker
 *                          $reason enum:
 *                              'decodingMessage'           解码非预定义消息时结构错误
 *                              'processingMessage'         使用用户提供的handler处理amqp消息时出错
 *                              'processingCustomMessage'   处理自定义消息出错
 *                              'unrecoverable'             当出现了不可恢复的错误,即将退出时
 */
class Worker extends AbstractWorker
{
    use TailingEventEmitterTrait;

    const STATE_RUNNING = 1;
    const STATE_SHUTTING = 2;
    const STATE_SHUTDOWN = 3;

    /**
     * @var callable
     */
    private $messageHandler;

    /**
     * @var int
     */
    private $state = self::STATE_SHUTDOWN;

    /**
     * @var int 已经收到的队列消息数量
     */
    private $received = 0;

    /**
     * @var bool
     */
    private $noMore = false;

    /**
     * @var bool 是否空闲退出
     */
    private $idleShutdown = false;

    /**
     * @var int 空闲退出的最长空闲时间(秒)
     */
    private $idleShutdownSec = 0;

    /**
     * @var TimerInterface
     */
    private $shutdownTimer = null;

    /**
     * @var bool 是否被动关闭(被动关闭是指由dispatcher杀死worker进程)
     */
    private $passiveShutdown = false;

    /**
     * @var float 进行一次巡逻的间隔周期(秒)
     */
    private $patrolPeriod = 60.0;

    public function __construct(string $id, $socketFD)
    {
        parent::__construct($id, $socketFD);

        $this->trySetShutdownTimer();
    }

    public function run()
    {
        if ($this->state !== self::STATE_SHUTDOWN) {
            return;
        }

        $this->errorlessEmit('start');

        $this->state = self::STATE_RUNNING;
        while ($this->state !== self::STATE_SHUTDOWN) {
            try {
                $this->process($this->patrolPeriod);
            } catch (\Throwable $e) {
                $this->errorlessEmit('error', ['unrecoverable', $e]);
                break;
            }

            if (!$this->getCommunicator()->isReadable() && !$this->getCommunicator()->isWritable()) {
                $this->errorlessEmit('disconnected');
                break;
            }

            $this->errorlessEmit('patrolling');
        }
    }

    /**
     * @param Message $msg
     *
     * @throws
     */
    public function handleMessage(Message $msg)
    {
        $this->clearShutdownTimer();

        switch ($msg->getType()) {
            case MessageTypeEnum::QUEUE:
                $this->received++;

                if (!$this->messageHandler) {
                    goto end;
                }

                try {
                    $decoded = $this->decodeMessage($msg->getContent());
                    $info = $decoded['meta']['amqp'] ?? [];
                    $info['content'] = $decoded['content'] ?? '';
                    $queueMsg = new QueueMessage($info);
                } catch (\Throwable $e) {
                    $this->errorlessEmit('error', ['decodingMessage', $e]);
                    goto end;
                }

                try {
                    call_user_func($this->messageHandler, $queueMsg, $this);
                } catch (\Throwable $e) {
                    $this->errorlessEmit('error', ['processingMessage', $e]);
                }

                end:
                $this->sendMessage(new Message(MessageTypeEnum::PROCESSED, ''));
                if ($this->state === self::STATE_SHUTTING) {
                    $this->sendMessage(new Message(MessageTypeEnum::STOP_SENDING, ''));
                }

                break;
            case MessageTypeEnum::LAST_MSG:
                $this->noMore = true;
                break;
            case MessageTypeEnum::ROGER_THAT:
                $this->state = self::STATE_SHUTDOWN;
                $this->stopProcess();
                $this->trySetShutdownTimer();
                return;
            default:
                try {
                    $this->emit('message', [$msg]);
                } catch (\Throwable $e) {
                    $this->errorlessEmit('error', ['processingCustomMessage', $e]);
                }
                $this->sendMessage(new Message(MessageTypeEnum::CUSTOM_MESSAGE_PROCESSED, ''));
                if ($this->state === self::STATE_SHUTTING) {
                    $this->sendMessage(new Message(MessageTypeEnum::STOP_SENDING, ''));
                }
        }

        $this->trySetShutdownTimer();

        if ($this->noMore) {
            if ($this->passiveShutdown) {
                $this->sendMessage(new Message(MessageTypeEnum::KILL_ME, ''));
            } else {
                $this->sendMessage(new Message(MessageTypeEnum::I_QUIT, ''));
            }
        }
    }

    /**
     * 发送自定义消息.
     *
     * @param Message $message
     *
     * @throws
     */
    public function sendCustomMessage(Message $message)
    {
        $this->sendMessage($message);
    }

    /**
     * 注册队列消息处理器.
     *
     * @param callable $h
     */
    public function setMessageHandler(callable $h)
    {
        $this->messageHandler = $h;
    }

    /**
     * 获得worker当前已经接收到的amqp消息数量.
     *
     * @return int
     */
    public function getReceivedNum(): int
    {
        return $this->received;
    }

    /**
     * 设置进程关闭模式.
     *
     * @param bool $isPassive true:被动模式, false:主动模式
     *
     * @return self
     */
    public function setShutdownMode(bool $isPassive): self
    {
        $this->passiveShutdown = $isPassive;

        return $this;
    }

    /**
     * 设置worker的空闲退出的最大空闲时间(秒).
     *
     * @param int $seconds 必须大于0,否则设置无效
     */
    public function setIdleShutdown(int $seconds)
    {
        if ($seconds <= 0) {
            return;
        }

        $this->idleShutdown = true;
        $this->idleShutdownSec = $seconds;
    }

    /**
     * 不再允许空闲退出.
     */
    public function noIdleShutdown()
    {
        $this->idleShutdown = false;
        $this->idleShutdownSec = 0;
        $this->clearShutdownTimer();
    }

    public function shutdown()
    {
        if ($this->state === self::STATE_RUNNING) {
            $this->state = self::STATE_SHUTTING;
        }
    }

    public function errorlessEmit(string $event, array $args = [])
    {
        try {
            $this->emit($event, $args);
        } finally {}
    }

    /**
     * 设置巡逻的间隔周期时间.
     *
     * @param int $seconds
     *
     * @return self
     */
    public function setPatrolPeriod(int $seconds): self
    {
        if ($seconds > 0) {
            $this->patrolPeriod = floatval($seconds);
        }

        return $this;
    }

    private function trySetShutdownTimer()
    {
        if (!$this->idleShutdown || $this->shutdownTimer) {
            return;
        }

        $this->shutdownTimer = $this->addTimer($this->idleShutdownSec, false, function () {
            $this->sendMessage(new Message(MessageTypeEnum::STOP_SENDING, ''));
        });
    }

    private function clearShutdownTimer()
    {
        if ($this->shutdownTimer) {
            $this->removeTimer($this->shutdownTimer);
            $this->shutdownTimer = null;
        }
    }

    /**
     * @param string $content
     * @return array
     * @throws \Exception
     */
    private function decodeMessage(string $content): array
    {
        $decoded = json_decode($content, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \Exception(sprintf("Error:%s, Content:%s", json_last_error_msg(), $content));
        }

        return $decoded;
    }
}