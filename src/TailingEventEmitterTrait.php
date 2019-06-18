<?php

declare(strict_types=1);

namespace Archman\BugsBunny;

trait TailingEventEmitterTrait
{
    public function emit($event, array $arguments = [])
    {
        // 非Whisper预定义事件就加上当前对象在参数末尾
        if (strpos($event, '__') !== 0) {
            $arguments[] = $this;
        }

        parent::emit($event, $arguments);
    }

    public function hasEventListened($event)
    {
        return isset($this->onceListeners[$event]) || isset($this->listeners[$event]);
    }
}