<?php

/**
 * 在示例01中,你可能会发现worker处理得尽管很快,worker的数量也很少,但是无论rabbitmq里有多少消息,消费速度始终会有一个上限.
 * 比如,在作者这台破机器上,每秒消费1200条左右的消息.
 * 你可能会说,我能保证我没有复杂业务,每条消息worker都能极速处理完,所以我不介意消息来得更多一些,多fork一些worker也没有关系.
 * 其实作者会推荐你多启动几个dispatcher来做这件事. 如果你不想,这里还有一个办法.
 *
 * 当在初始化Connection的时候,你可以对Connection示例调用setPrefetch方法,它其实是更改amqp channel的qos prefetch值
 * 这要会在一定提高消费能力,但是作者测试时发现会边际递减的效应. 所以作者还是推荐使用上面的方法,多启动几个dispatcher吧.
 */

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;

$factory = new WorkerFactory();
$factory->setMessageHandler(function (QueueMessage $message, Worker $worker) {
    // 处理能力超好
    usleep(100);
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

// !!!!!!!!!!就是这里!!!!!!!!!!!!
$conn->setPrefetch(10);

$dispatcher = new Dispatcher($conn, $factory);

$dispatcher->on('processed', function (string $workerID, Dispatcher $dispatcher) {
    $stat = $dispatcher->getStat();
    $processed = $stat['processed'];
    $consumed = $stat['consumed'];

    echo "{$processed}/{$consumed} Worker {$workerID} Has Processed A Message, Workers:{$dispatcher->countWorkers()}.\n";
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