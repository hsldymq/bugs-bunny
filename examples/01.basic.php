<?php

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;

$factory = new WorkerFactory();
$factory->setMessageHandler(function (QueueMessage $message, Worker $worker) {
    // 模拟正常处理逻辑
    usleep(1000);
});
$factory->registerSignal(SIGINT, function () {
    // Ctrl+C会向前端进程组发送SIGINT信号,我们不希望worker也被这个信号影响,而是希望由dispatcher来控制它的生存周期
});
$factory->registerEvent('error', function (string $reason, \Throwable $e, Worker $worker) {
    echo "Worker Error. Reason: {$reason}, Message: {$e->getMessage()}\n";
    $worker->shutdown();
});

$params = require __DIR__.'/amqp_params.php';
$conn = new Connection($params['connectionOptions'], $params['queues']);

$dispatcher = new Dispatcher($conn, $factory);

$dispatcher->on('processed', function (string $workerID, Dispatcher $dispatcher) {
    $stat = $dispatcher->getStat();
    $processed = $stat['processed'];
    $consumed = $stat['consumed'];

    echo "{$processed}/{$consumed} Worker {$workerID} Has Processed The Message, Workers:{$dispatcher->countWorkers()}.\n";
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

echo ' [*] Waiting for messages. To exit press CTRL+C', "\n";
$dispatcher->run();