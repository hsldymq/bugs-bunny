<?php

/**
 * 在这个示例中,我们演示worker主动调用shutdown结束运行.
 *
 * 在示例03中,我们展示了worker空闲退出的能力.
 * 但是在生产环境中,我们的worker可能因为业务的持续繁忙没有机会空闲退出.
 * 而在各种业务逻辑操作下,可能会大量持续的消耗内存甚至是内存泄露.
 * 我们不希望worker的持续运行造成其内存使用达到上限而崩溃.
 * 所以你根据某种条件是否不再处理更多消息,并主动结束.
 */

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;

echo "Waiting for messages. To exit press CTRL+C\n";

$factory = (new WorkerFactory())
    ->setMessageHandler(function (QueueMessage $message, Worker $worker) {
        // 处理业务逻辑

        // 这里你可以根据已接受处理的消息数量来决定是否退出
        // 也可以使用memory_get_usage函数来检查是否达到一个临界值,判断条件取决于你自己
        if ($worker->getReceivedNum() >= 1000) {
            $worker->shutdown();
        }
    })
    ->registerEvent('error', function (string $reason, \Throwable $e, Worker $worker) {
        echo "Worker Error. Reason: {$reason}, Message: {$e->getMessage()}\n";
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
    })
    ->addSignalHandler(SIGINT, function (int $signal, Dispatcher $dispatcher) {
        $dispatcher->shutdown();
    })
    ->run();