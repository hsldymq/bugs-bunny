<?php

namespace Archman\BugsBunny;

use Archman\Whisper\AbstractMaster;
use Archman\Whisper\Message;

class Dispatcher extends AbstractMaster
{
    /**
     * @var int $processorLimit 子进程数量上线
     *
     * 当子进程到达上线后,调度器会停止接受消息队列的消息.
     * 等待有子进程闲置后,继续派发消息.
     */
    private $processorLimit;

    /**
     * 设置进程数量上限.
     *
     * @param int $num >0时为上限值, =-1时为不限数量, 0或者<-1时不操作.
     */
    public function setProcessorLimit(int $num)
    {
        if ($num > 0 || $num === -1) {
            $this->processorLimit = $num;
        }
    }

    public function run()
    {
        // TODO
    }

    public function onMessage(string $workerID, Message $msg)
    {
        // TODO
    }

}