<?php

/**
 * 在这个示例中,展示了最基本的流程.
 * 1. 向WorkerFactory注册worker的事件处理器,信号处理器以及最关键的amqp消息处理器
 * 2. Dispatcher实际上就是一个master,它监听来自Connection的amqp消息,并将消息投放给当前空闲的worker进程,如果尚无空闲,就fork一个
 *    fork的进程使用WorkerFactory来创建一个Worker实例来处理所有投放过来的amqp消息
 *    Dispatcher也可以注册/监听事件,信号处理器
 *
 * 这个示例不限制fork的worker数量,所以你可以调整usleep的值,来看看在海量消息到来的时候,worker的消息处理能力对fork数量的影响.
 * 建议不要把值设得太高,否则系统资源可能会被fork耗完,但在生产环境下,我们不可能左右worker的处理能力,必然可能会出现大量fork的情况.
 *
 * 示例02中,你会看到如何限制worker的数量上限.
 */

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;

$factory = (new WorkerFactory())
    ->setMessageHandler(function (QueueMessage $message, Worker $worker) {
        // 模拟正常处理逻辑
        usleep(1000);
    })
    ->registerEvent('error', function (string $reason, \Throwable $e, Worker $worker) {
        echo "Worker Error. Reason: {$reason}, Message: {$e->getMessage()}\n";
        $worker->shutdown();
    });

$params = require __DIR__.'/amqp_params.php';
$conn = new Connection($params['connectionOptions'], $params['queues']);

$dispatcher = (new Dispatcher($conn, $factory))
    ->on('processed', function (string $workerID, Dispatcher $dispatcher) {
        $stat = $dispatcher->getStat();
        $processed = $stat['processed'];
        $consumed = $stat['consumed'];
        $memory = number_format(memory_get_usage());

        echo "{$processed}/{$consumed} Worker {$workerID} Has Processed A Message, Workers:{$dispatcher->countWorkers()}, Memory Usage:{$memory} Bytes.\n";
    })
    ->on('workerExit', function (string $workerID, int $pid, Dispatcher $dispatcher) {
        $count = $dispatcher->countWorkers();
        echo "Worker {$workerID} Quit, PID: {$pid}, {$count} Remains.\n";
    })
    ->on('error', function (string $reason, \Throwable $e, Dispatcher $dispatcher) {
        echo "Dispatcher Error, Reason: {$reason}, Message: {$e->getMessage()}\n";
        $dispatcher->shutdown();
    })
    ->on('shutdown', function (Dispatcher $dispatcher) {
        $stat = $dispatcher->getStat();
        echo "\n";
        echo "Consumed Message: {$stat['consumed']}.\n";
        echo "Processed Message: {$stat['processed']}.\n";
        echo "Max Queue Message Length: {$stat['maxMessageLength']} Bytes.\n";
        echo "Peak Number Of Workers: {$stat['peakNumWorkers']}.\n";
        echo "Peak Number Of Cached Messages: {$stat['peakNumCached']}.\n";
        echo "Peak Memory Usage: ".number_format(memory_get_peak_usage()).' Bytes.'.PHP_EOL;
    });
$dispatcher->addSignalHandler(SIGINT, function () use ($dispatcher) {
    $dispatcher->shutdown();
});

echo "Waiting for messages. To exit press CTRL+C\n";
$dispatcher->run();