<?php

/**
 * 在这个示例中,我们模拟worker在面临复杂业务逻辑或因为其他IO阻塞的情况下导致处理事件过长的情况.
 * 如果大量消息到来,dispatcher就会fork大量的worker来处理,势必大量消耗系统资源.
 *
 * 为了防止这种情况发生,可以为dispatcher设置fork worker的上限数量来达到这个目的.
 * 下面的代码与示例01大同小异,但我们调用了setMaxWorkers设置worker上线,并调用setCacheLimit允许在worker忙碌的时候缓存一些消息,待有空闲的时候直接派发.
 *
 * 在这个示例里,如果在运行过程中你按CTRL+C试图停止运行,你会发现它有时候并不会立刻停止,而是会试图在运行片刻时间后才停止.
 * 这是因为dispatcher会将缓存起来的消息分发完之后才结束运行, 这样我们才能确保消费的每一条消息都能够被worker给处理掉而不遗漏.
 *
 * 那么问题来了,我们考虑了worker在高负载情况下的数量限制,那么当从高负载情形中恢复后,可能并不需要那么多worker来处理
 * 这时势必就会出现有些worker一直处于闲置状态,但我们不想浪费不必要的系统资源,怎么办呢?
 *
 * 在示例03中,可以看到我们能够解决这个问题.
 */

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;

$factory = new WorkerFactory();
$factory->setMessageHandler(function (QueueMessage $message, Worker $worker) {
    // 处理200ms,在消息量很多时,这会造成Dispatcher大量fork出worker
    usleep(200000);
});
$factory->registerSignal(SIGINT, function () {
    // Ctrl+C会向前端进程组发送SIGINT信号,我们不希望worker也被这个信号影响,而是希望由dispatcher来控制它的生存周期
});
$factory->registerEvent('error', function (string $reason, \Throwable $e, Worker $worker) {
    echo "Worker Error. Reason: {$reason}, Message: {$e->getMessage()}\n";
});


$params = require __DIR__.'/amqp_params.php';
$conn = new Connection($params['connectionOptions'], $params['queues']);

$dispatcher = new Dispatcher($conn, $factory);

// 这里将worker限制在一定量,这会降低消费速度,但是不会因为fork将系统资源耗完
$dispatcher->setMaxWorkers(50);
// 设置缓存的消息数量,当worker创建满后,会预先消费一些消息放到缓存中,待有worker空闲时,按照FIFO优先派发缓存的消息
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