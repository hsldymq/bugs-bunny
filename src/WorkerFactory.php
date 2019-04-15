<?php

declare(strict_types=1);

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractWorker;
use Archman\Whisper\Interfaces\WorkerFactoryInterface;

class WorkerFactory implements WorkerFactoryInterface
{
    /**
     * @var array [
     *      [$signal, $handler],
     *      ...
     * ]
     */
    private $signalHandlers = [];

    /**
     * @var array [
     *      [$event, $handler],
     *      ...
     * ]
     */
    private $eventHandlers = [];

    /**
     * @var callable
     */
    private $msgHandler;

    /**
     * @var null|int
     */
    private $idleShutdownSec = null;

    public function makeWorker(string $id, $socketFD): AbstractWorker
    {
        $worker = new Worker($id, $socketFD);

        foreach ($this->signalHandlers as $each) {
            $worker->addSignalHandler($each[0], $each[1]);
        }

        foreach ($this->eventHandlers as $each) {
            $worker->on($each[0], $each[1]);
        }

        if ($this->msgHandler) {
            $worker->setMessageHandler($this->msgHandler);
        }

        if ($this->idleShutdownSec !== null) {
            $worker->setIdleShutdown($this->idleShutdownSec);
        }

        return $worker;
    }

    /**
     * 注册worker的信号处理器.
     *
     * @param int $signal
     * @param callable $handler
     *
     * @return self
     */
    public function registerSignal(int $signal, callable $handler): self
    {
        $this->signalHandlers[] = [$signal, $handler];

        return $this;
    }

    /**
     * 注册worker的事件
     *
     * @param string $event
     * @param callable $handler
     *
     * @return self
     */
    public function registerEvent(string $event, callable $handler): self
    {
        $this->eventHandlers[] = [$event, $handler];

        return $this;
    }

    /**
     * 设置队列消息处理器.
     *
     * @param callable $handler
     *
     * @return self
     */
    public function setMessageHandler(callable $handler): self
    {
        $this->msgHandler = $handler;

        return $this;
    }

    /**
     * 设置worker空闲退出的最大空闲时间(秒).
     *
     * @param int $seconds 必须大于0,否则设置无效
     *
     * @return self
     */
    public function setIdleShutdown(int $seconds): self
    {
        if ($seconds > 0) {
            $this->idleShutdownSec = $seconds;
        }

        return $this;
    }
}