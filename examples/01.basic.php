<?php

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;

$factory = new WorkerFactory();
$factory->setMessageHandler(function (QueueMessage $message, Worker $worker) {
    // 1ms ~ 100ms
    // 模拟正常处理逻辑
    usleep(mt_rand(1000, 100000));
});

$dispatcher = new Dispatcher([
    'host' => '127.0.0.1',
    'port' => 5672,
    'vhost' => '/',
    'user' => 'guest',
    'password' => 'guest',
], $factory);
$dispatcher->connect(['queue1', 'queue2', 'queue3']);

$dispatcher->on('processed', function (string $workerID, Dispatcher $master) {
    $stat = $master->getStat();
    $process = $stat['processed'];
    $consumed = $stat['consumed'];

    echo "{$process}/{$consumed} Master - Worker {$workerID} Has Processed The Message, Workers:{$master->countWorkers()}.\n";
});

$dispatcher->on('workerQuit', function (string $workerID, Dispatcher $dispatcher) {
    $count = $dispatcher->countWorkers() - 1;
    echo "Worker {$workerID} Quit, {$count} Remains.\n";
});

$dispatcher->addSignalHandler(SIGINT, function () use ($dispatcher) {
    $stat = $dispatcher->getStat();
    echo "\n";
    echo "Consumed Message: {$stat['consumed']}\n";
    echo "Processed Message: {$stat['processed']}\n";
    echo "Peak Worker Number: {$stat['peakWorkerNum']}\n";
    echo "Max Memory Usage: ".number_format($stat['memoryUsage']).' Bytes'.PHP_EOL;
    echo "Max Peak Memory Usage: ".number_format($stat['peakMemoryUsage']).' Bytes'.PHP_EOL;
    $dispatcher->shutdown();
});

echo ' [*] Waiting for messages. To exit press CTRL+C', "\n";
$dispatcher->run();