<?php

declare(strict_types=1);

namespace Archman\BugsBunny\Interfaces;

use Evenement\EventEmitterInterface;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;

interface AMQPConnectionInterface
{
    public function connect(LoopInterface $eventLoop, ConsumerHandlerInterface $handler): PromiseInterface;

    public function disconnect(): PromiseInterface;

    public function pause(): PromiseInterface;

    public function resume(): PromiseInterface;

    public function isConnected(): bool;
}