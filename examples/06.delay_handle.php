<?php

/**
 * 在这个示例中,展示了Worker提供的一种延迟处理消息的能力.
 * 我们在Worker的start event中调用setDelayCondition注册延迟判断函数
 * 这个函数返回一个整型值代表延迟的时间(毫秒),0代表不延迟,立刻处理
 * 当存在延迟处理的消息时,Worker不会受闲置退出设置的影响
 * 也就是说当一个消息延迟处理的时间高于闲置退出的时间,也会等到所有消息处理完成之后才会进行闲置退出判断
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
    ->registerSignal(SIGINT, function () {
        // Ctrl+C会向前端进程组发送SIGINT信号,我们不希望worker也被这个信号影响,而是希望由dispatcher来控制它的生存周期
    })
    // 闲置5秒后退出
    ->setIdleShutdown(5)
    ->registerEvent('start', function (Worker $worker) {
        $worker->setDelayCondition(function (QueueMessage $msg): int {
            // 延迟10秒处理,尽管比闲置退出时间更长,但worker始终会在所有延迟处理的消息完后,在进入闲置退出的程序
            return 10000;
        });
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
    echo "shutdown\n";
    $dispatcher->shutdown();
});

echo "Waiting for messages. To exit press CTRL+C\n";
$dispatcher->run();