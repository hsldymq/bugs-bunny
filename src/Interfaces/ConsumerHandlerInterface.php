<?php

declare(strict_types=1);

namespace Archman\BugsBunny\Interfaces;

use Bunny\Async\Client;
use Bunny\Channel;
use Bunny\Message as AMQPMessage;

interface ConsumerHandlerInterface
{
    public function onConsume(
        AMQPMessage $AMQPMessage,
        string $queue,
        Channel $channel,
        Client $client
    ): void;
}
