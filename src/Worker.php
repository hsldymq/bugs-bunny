<?php

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractWorker;
use Archman\Whisper\Message;
use Psr\Log\LoggerInterface;
use React\EventLoop\TimerInterface;

/**
 * @event message
 * @event errorDecodingMessage
 * @event errorProcessingMessage
 */
class Worker extends AbstractWorker
{
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var callable
     */
    private $messageHandler;

    /**
     * @var string running / shutting
     */
    private $state = 'running';

    /**
     * @var int 已经收到的队列消息数量
     */
    private $receive = 0;

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

    public function handleMessage(Message $msg)
    {
        $this->clearShutdownTimer();

        $msgType = $msg->getType();
        $cnt = $msg->getContent();

        $contentArray = json_decode($cnt, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            if ($this->logger) {
                $this->logger->error("Worker failed to decode message: ".json_last_error_msg(), ['content' => $cnt]);
            }
            $this->emit('errorDecodingMessage', [$cnt, $this]);
            $this->trySetShutdownTimer();

            return;
        }

        switch ($msgType) {
            case MessageTypeEnum::QUEUE:
                $this->receive++;

                if ($this->logger) {
                    $this->logger->debug("Queue message incoming", ['content' => $cnt]);
                }

                if (!$this->messageHandler) {
                    goto ending;
                }

                $info = array_merge(
                    $contentArray['meta']['amqp'] ?? [],
                    ['content' => $contentArray['content'] ?? '']
                );
                try {
                    $queueMsg = new QueueMessage($info);
                    call_user_func($this->messageHandler, $queueMsg, $this);
                } catch (\Throwable $e) {
                    if ($this->logger) {
                        $this->logger->error("Worker failed to process message: {$e->getMessage()}", [
                            'content' => $info
                        ]);
                    }
                    $this->emit('errorProcessingMessage', [$cnt, $this]);
                }

                ending:
                $this->sendMessage(new Message(MessageTypeEnum::PROCESSED, ''));
                if ($this->state === 'shutting') {
                    $this->sendMessage(new Message(MessageTypeEnum::STOP_SENDING, ''));
                }

                break;
            case MessageTypeEnum::LAST_MSG:
                $this->noMore = true;
                break;
            default:
                try {
                    $this->emit('message', [$msg, $this]);
                } catch (\Throwable $e) {
                    if ($this->logger) {
                        $this->logger->error("Handle delegate message($msgType) error: {$e->getMessage()}", [
                            'content' => $msg->getContent(),
                        ]);
                    }
                }

        }

        $this->trySetShutdownTimer();

        $sent = $contentArray['meta']['sent'] ?? -1;
        if ($this->noMore && $this->receive === $sent) {
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
    }

    /**
     * 不再允许空闲退出.
     */
    public function noIdleShutdown()
    {
        $this->idleShutdown = false;
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
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function shutdown()
    {
        if ($this->state === 'running') {
            $this->state = 'shutting';
        }
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
}