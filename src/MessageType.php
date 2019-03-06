<?php

namespace Archman\BugsBunny;

class MessageType
{
    // 正常队列消息
    const QUEUE = 0;

    // 子进程已处理完消息
    const PROCESSED = 1;

    // 子进程告知主进程不要投放新消息
    const STOP_SENDING = 2;

    // 主进程告知子进程不再向他发送消息
    const LAST_MSG  = 3;

    // 子进程处理完所有内容,请求主进程杀死自己
    const KILL_ME   = 4;
}