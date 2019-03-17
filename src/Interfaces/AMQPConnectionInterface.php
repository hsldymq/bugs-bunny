<?php

namespace Archman\BugsBunny\Interfaces;

use React\EventLoop\LoopInterface;

interface AMQPConnectionInterface
{
    public function connect(LoopInterface $eventLoop, ConsumerHandlerInterface $handler);

    public function disconnect();

    public function pause();

    public function resume();
}