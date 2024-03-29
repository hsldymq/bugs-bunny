<?php

declare(strict_types=1);

namespace Archman\BugsBunny;

class MessageTypeEnum
{
    // 正常队列消息
    public const QUEUE = 0;

    // worker已处理完队列消息
    public const PROCESSED = 1;

    // worker告知dispatcher不要投放新消息
    public const STOP_SENDING = 2;

    // dispatcher告知worker不再向他发送消息
    public const LAST_MSG = 3;

    // 对于被动退出模式,worker进程处理完所有内容,请求dispatcher杀死自己
    public const KILL_ME = 4;

    // 对于主动退出模式,worker进程通知dispatcher准备退出
    public const I_QUIT = 5;

    // 对于主动退出模式,dispatcher收到I_QUIT消息后,告知dispatcher准备好接受worker的退出
    public const ROGER_THAT = 6;

    // worker处理完自定义消息
    public const CUSTOM_MESSAGE_PROCESSED = 8;
}