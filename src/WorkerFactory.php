<?php

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractWorker;
use Archman\Whisper\Interfaces\WorkerFactoryInterface;

class WorkerFactory implements WorkerFactoryInterface
{
    public function makeWorker(string $id, $socketFD): AbstractWorker
    {
        return new Processor($id, $socketFD);
    }
}