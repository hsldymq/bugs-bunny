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

    public function makeWorker(string $id, $socketFD): AbstractWorker
    {
        $p = new Processor($id, $socketFD);

        foreach ($this->signalHandlers as $s => $h) {
            $p->addSignalHandler($s, $h);
        }

        foreach ($this->eventHandlers as $e => $h) {
            $p->on($e, $h);
        }

        return $p;
    }

    public function registerSignal(int $signal, callable $handler)
    {
        $this->signalHandlers[] = [$signal, $handler];
    }

    public function registerEvent(string $event, callable $handler)
    {
        $this->eventHandlers[] = [$event, $handler];
    }
}