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

    /**
     * @var null|bool
     */
    private $isPassiveShutdown = null;

    /**
     * @var null|int
     */
    private $patrolPeriod = null;

    /**
     * @var bool
     */
    private $captureSignal = true;

    public function makeWorker(string $id, $socketFD): AbstractWorker
    {
        $worker = new Worker($id, $socketFD);

        if ($this->captureSignal) {
            // 在以非daemon方式运行的情况下,子进程作为前台进程组会收到SIGINT,SIGQUIT而非正常退出,所以需要捕获该信号
            if (defined('SIGINT')) {
                $worker->addSignalHandler(SIGINT, function () {});
            }

            if (defined('SIGQUIT')) {
                $worker->addSignalHandler(SIGQUIT, function () {});
            }
        }


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

        if ($this->isPassiveShutdown !== null) {
            $worker->setShutdownMode($this->isPassiveShutdown);
        }

        if ($this->patrolPeriod !== null) {
            $worker->setPatrolPeriod($this->patrolPeriod);
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

    /**
     * 设置worker关闭为被动关闭模式还是主动关闭模式.
     *
     * 主动关闭是指worker进程结束事件循环,退出主逻辑最后脚本结束
     * 被动关闭是指worker告知dispatcher让其通过信号杀死自己
     *
     * 默认是主动关闭模式
     * 由于使用grpc 1.20以下版本的扩展时,fork的子进程无法正常结束,这里提供了一种被动关闭机制来防止这种情况方式.
     *
     * @param bool $isPassive
     *
     * @return self
     */
    public function setShutdownMode(bool $isPassive): self
    {
        $this->isPassiveShutdown = $isPassive;

        return $this;
    }

    /**
     * 设置Worker的巡逻的间隔周期时间.
     *
     * @param int $seconds
     *
     * @return self
     */
    public function setPatrolPeriod(int $seconds): self
    {
        if ($seconds > 0) {
            $this->patrolPeriod = $seconds;
        }

        return $this;
    }

    /**
     * 默认情况下,worker进程会捕获SIGINT和SIGQUIT信号,防止被信号直接杀死.
     *
     * @param bool $c 传递false不默认捕获这两个信号
     *
     * @return self
     */
    public function setCaptureSignal(bool $c): self
    {
        $this->captureSignal = $c;

        return $this;
    }
}