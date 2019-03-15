<?php

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;

$factory = new WorkerFactory();
$factory->setMessageHandler(function (QueueMessage $message, Worker $worker) {
    // 10us ~ 100us
    // 模拟正常处理逻辑
    usleep(mt_rand(10, 100));
});

$conn = new Connection(['host' => '127.0.0.1',
    'port' => 5672,
    'vhost' => '/',
    'user' => 'guest',
    'password' => 'guest',
], ['queue1', 'queue2', 'queue3']);

$dispatcher = new Dispatcher($conn, $factory);

$dispatcher->on('processed', function (string $workerID, Dispatcher $master) {
    $stat = $master->getStat();
    $process = $stat['processed'];
    $consumed = $stat['consumed'];

    echo "{$process}/{$consumed} Master - Worker {$workerID} Has Processed The Message, Workers:{$master->countWorkers()}.\n";
});

$dispatcher->on('workerQuit', function (string $workerID, int $pid, Dispatcher $dispatcher) {
    $count = $dispatcher->countWorkers();
    echo "Worker {$workerID} Quit, PID: {$pid}, {$count} Remains.\n";
});

$dispatcher->on('shutdown', function (Dispatcher $dispatcher) {
    $stat = $dispatcher->getStat();
    echo "\n";
    echo "Consumed Message: {$stat['consumed']}\n";
    echo "Processed Message: {$stat['processed']}\n";
    echo "Peak Worker Number: {$stat['peakWorkerNum']}\n";
    echo "Peak Memory Usage: ".number_format(memory_get_peak_usage()).' Bytes'.PHP_EOL;
});

$dispatcher->addSignalHandler(SIGINT, function () use ($dispatcher) {
    $dispatcher->shutdown();
});

echo ' [*] Waiting for messages. To exit press CTRL+C', "\n";
$dispatcher->run();