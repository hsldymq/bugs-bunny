<?php

/**
 * 在这个示例中,展示了发送发送自定义消息的能力.
 *
 * 有时候我们可能需要在消费的同时定时定量的做一些额外的操作,例如统计运行时数据或消费数据,或者定时归档一些队列消费相关的数据
 * 我们可以让dispatcher的向任意空闲worker发送一个自定义消息,让其处理这些额外的操作
 * 还可以利用dispatcher的patrolling事件,我们就能定时让worker处理这样的事情.
 */

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;
use Archman\Whisper\Message;

$factory = (new WorkerFactory())
    // 向worker注册message事件,捕获dispatcher发来的自定义消息
    ->registerEvent('message', function (Message $msg, Worker $worker) {
        // 编写具体的自定义事件的处理逻辑:
        // switch ($msg->getType()) {
        //      case ...:
        // }
        sleep(1);
    })
    ->registerEvent('error', function (string $reason, \Throwable $e, Worker $worker) {
        echo "Worker Error. Reason: {$reason}, Message: {$e->getMessage()}\n";
        $worker->shutdown();
    });

$params = require __DIR__.'/amqp_params.php';
$conn = new Connection($params['connectionOptions'], $params['queues']);

$dispatcher = (new Dispatcher($conn, $factory))
    // 为了方便展示效果,这里每一秒就会触发一次patrolling事件
    ->setPatrolPeriod(1)
    // 我们在patrolling事件中就可以做一些定时的任务,例如通知worker进行一次数据采样或者统计计算等几
    ->on('patrolling', function (Dispatcher $dispatcher) {
        $dispatcher->dispatchCustomMessage(new Message(100, 'this is a custom message'));
    })
    // worker处理完自定义消息后会触发customMessageProcessed事件
    ->on('customMessageProcessed', function (string $workerID, Dispatcher $dispatcher) {
        $memory = number_format(memory_get_usage());

        echo "[Custom Message] Worker {$workerID} Has Processed A Message, Workers:{$dispatcher->countWorkers()}, Memory Usage:{$memory} Bytes.\n";
    })
    // 也可以在dispatcher启动的时候让worker进行一些初始化操作
    ->on('start', function (Dispatcher $dispatcher) {
        $dispatcher->dispatchCustomMessage(new Message(101, 'dispatcher started'));
    })
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