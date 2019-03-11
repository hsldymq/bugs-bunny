<?php

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

    public function makeWorker(string $id, $socketFD): AbstractWorker
    {
        $p = new Worker($id, $socketFD);

        foreach ($this->signalHandlers as $s => $h) {
            $p->addSignalHandler($s, $h);
        }

        foreach ($this->eventHandlers as $e => $h) {
            $p->on($e, $h);
        }

        if ($this->msgHandler) {
            $this->setMessageHandler($this->msgHandler);
        }

        return $p;
    }

    public function registerSignal(int $signal, callable $handler)
    {
        $this->signalHandlers[] = [$signal, $handler];
    }

    public function clearSignal()
    {
        $this->signalHandlers = [];
    }

    public function registerEvent(string $event, callable $handler)
    {
        $this->eventHandlers[] = [$event, $handler];
    }

    public function clearEvent()
    {
        $this->eventHandlers = [];
    }

    public function setMessageHandler(callable $handler)
    {
        $this->msgHandler = $handler;
    }
}