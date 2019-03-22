<?php

/**
 * 在这个示例中,我们会模拟worker从高负载状态下会恢复回来,闲置的worker会退出的情形.
 *
 * 由于高负载的原因,worker数量跑到了上限,当恢复到低负载后,我们并不需要那么多worker来处理消息.
 * 如果不提供一种退出机制,那么worker会一直存在而占用不必要的系统资源.
 *
 * bugs-bunny提供了setIdleShutdown方法来做实现这样的机制.
 * 你只需要传递闲置的时间(秒), 当worker闲置这么久后,会自动退出.
 *
 * 你可以看到,由于我们设置的worker上限是50个,在前2500条消息时,worker数量会一致保持在上限的状态.
 * 此后worker延迟变低,多数worker变得空闲,空闲worker会在几秒后退出,worker数量急剧减少.
 *
 * 当所有消息都消费并处理完后,剩余的worker空闲几秒后也退出了,dispatcher变成了一个"光杆司令".
 */

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;

$factory = new WorkerFactory();
$factory->setMessageHandler(function (QueueMessage $message, Worker $worker) {
    // 这里我们模拟业务逻辑在前期遭遇一些情况造成处理时间过长,后期正常恢复
    if ($worker->getReceivedNum() < 50) {
        usleep(200000);
    } else {
        usleep(3000);
    }
});
// 当worker空闲5秒后会不再接收消息,并通知dispatcher结束它的生命周期
$factory->setIdleShutdown(5);
$factory->registerSignal(SIGINT, function () {
    // Ctrl+C会向前端进程组发送SIGINT信号,我们不希望worker也被这个信号影响,而是希望由dispatcher来控制它的生存周期
});
$factory->registerEvent('error', function (string $reason, \Throwable $e, Worker $worker) {
    echo "Worker Error. Reason: {$reason}, Message: {$e->getMessage()}\n";
});


$params = require __DIR__.'/amqp_params.php';
$conn = new Connection($params['connectionOptions'], $params['queues']);

$dispatcher = new Dispatcher($conn, $factory);

// 这里依旧如示例02一样,设置worker上限,将数量控制在一个安全的范围内
$dispatcher->setMaxWorkers(50);
// 设置缓存数量
$dispatcher->setCacheLimit(1000);

$dispatcher->on('processed', function (string $workerID, Dispatcher $dispatcher) {
    $stat = $dispatcher->getStat();
    $processed = $stat['processed'];
    $consumed = $stat['consumed'];

    echo "{$processed}/{$consumed} - Worker {$workerID} Has Processed A Message, Workers:{$dispatcher->countWorkers()}, Idle:{$dispatcher->countSchedulable()}.\n";
});

$dispatcher->on('workerExit', function (string $workerID, int $pid, Dispatcher $dispatcher) {
    $count = $dispatcher->countWorkers();
    echo "Worker {$workerID} Quit, PID: {$pid}, {$count} Remains.\n";
});

$dispatcher->on('error', function (string $reason, \Throwable $e, Dispatcher $dispatcher) {
    echo "Dispatcher Error, Reason: {$reason}, Message: {$e->getMessage()}\n";
    $dispatcher->shutdown();
});

$dispatcher->on('shutdown', function (Dispatcher $dispatcher) {
    $stat = $dispatcher->getStat();
    echo "\n";
    echo "Consumed Message: {$stat['consumed']}\n";
    echo "Processed Message: {$stat['processed']}\n";
    echo "Peak Number Of Workers: {$stat['peakNumWorkers']}\n";
    echo "Peak Number Of Cached Messages: {$stat['peakNumCached']}\n";
    echo "Peak Memory Usage: ".number_format(memory_get_peak_usage()).' Bytes'.PHP_EOL;
});

$dispatcher->addSignalHandler(SIGINT, function () use ($dispatcher) {
    $dispatcher->shutdown();
});

echo "Waiting for messages. To exit press CTRL+C\n";
$dispatcher->run();