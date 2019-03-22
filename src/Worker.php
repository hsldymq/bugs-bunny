<?php

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractWorker;
use Archman\Whisper\Message;
use React\EventLoop\TimerInterface;

/**
 * 可以使用on方法或在WorkerFactory使用registerEvent方法监听以下预定义事件:
 * @event start             worker启动
 *                          参数: Worker $worker
 *
 * @event message           dispatcher发来自定义消息时
 *                          参数: \Archman\Whisper\Message $msg, Worker $worker
 *
 * @event disconnected      与dispatcher的连接中断,即将退出
 *                          参数: Worker $worker
 *
 * @event error             发生错误
 *                          参数: string $reason, \Throwable $ex, Worker $worker
 *                          $reason enum:
 *                              'decodingMessage'       解码非预定义消息时结构错误
 *                              'processingMessage'     使用用户提供的handler处理amqp消息时出错
 *                              'unrecoverable'         当出现了不可恢复的错误,即将退出时
 */
class Worker extends AbstractWorker
{
    use EventEmitterTrait;

    /**
     * @var callable
     */
    private $messageHandler;

    /**
     * @var string running / shutting / shutdown
     */
    private $state = 'shutdown';

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

    public function __construct(string $id, $socketFD)
    {
        parent::__construct($id, $socketFD);

        $this->trySetShutdownTimer();
    }

    public function run()
    {
        if ($this->state !== 'shutdown') {
            return;
        }

        $this->errorlessEmit('start');

        $this->state = 'running';
        while (true) {
            try {
                $this->process(60);
            } catch (\Throwable $e) {
                $this->errorlessEmit('error', ['unrecoverable', $e]);
                break;
            }

            if (!$this->getCommunicator()->isReadable() && !$this->getCommunicator()->isWritable()) {
                $this->errorlessEmit('disconnected');
                break;
            }
        }
    }

    public function handleMessage(Message $msg)
    {
        $this->clearShutdownTimer();

        $msgType = $msg->getType();
        $cnt = $msg->getContent();

        if (in_array($msgType, [MessageTypeEnum::QUEUE, MessageTypeEnum::LAST_MSG])) {
            try {
                $decodedMsg = $this->decodeMessage($cnt);
            } catch (\Throwable $e) {
                $this->errorlessEmit('error', ['decodingMessage', $e]);
                $this->trySetShutdownTimer();
                return;
            }
        }

        switch ($msgType) {
            case MessageTypeEnum::QUEUE:
                $this->received++;

                if (!$this->messageHandler) {
                    goto end;
                }

                try {
                    $info = $decodedMsg['meta']['amqp'] ?? [];
                    $info['content'] = $decodedMsg['content'] ?? '';
                    $queueMsg = new QueueMessage($info);
                } catch (\Throwable $e) {
                    $this->errorlessEmit('error', ['decodingMessage', $e]);
                    goto end;
                }

                try {
                    call_user_func($this->messageHandler, $queueMsg, $this);
                } catch (\Throwable $e) {
                    $this->errorlessEmit('error', ['processingMessage', $cnt]);
                }

                end:
                $this->sendMessage(new Message(MessageTypeEnum::PROCESSED, ''));
                if ($this->state === 'shutting') {
                    $this->sendMessage(new Message(MessageTypeEnum::STOP_SENDING, ''));
                }

                break;
            case MessageTypeEnum::LAST_MSG:
                $this->noMore = true;
                break;
            default:
                $this->errorlessEmit('message', [$msg]);
        }

        $this->trySetShutdownTimer();

        $sent = $decodedMsg['meta']['sent'] ?? null;
        if ($this->noMore && $this->received === $sent) {
            $this->sendMessage(new Message(MessageTypeEnum::KILL_ME, ''));
        }
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

    public function shutdown()
    {
        if ($this->state === 'running') {
            $this->state = 'shutting';
        }
    }

    public function errorlessEmit(string $event, array $args = [])
    {
        try {
            $this->emit($event, $args);
        } finally {}
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