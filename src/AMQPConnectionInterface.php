<?php

namespace Archman\BugsBunny;

use React\EventLoop\LoopInterface;

interface AMQPConnectionInterface
{
    public function connect(LoopInterface $eventLoop, callable $consumeHandler);

    public function disconnect();

    public function pause();

    public function resume();

    public function getQueue(string $tag);
}