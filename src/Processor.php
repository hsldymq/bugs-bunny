<?php

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractWorker;
use Archman\Whisper\Message;

class Processor extends AbstractWorker
{
    /** @var callable */
    private $messageHandler;

    public function setMessageHandler(callable $h)
    {
        $this->messageHandler = $h;
    }

    public function handleMessage(Message $msg)
    {
        if ($this->messageHandler) {
            call_user_func($this->messageHandler, $msg, $this);
        }
    }
}