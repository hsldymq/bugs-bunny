<?php

require_once __DIR__.'/../vendor/autoload.php';

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;

$factory = new WorkerFactory();
$factory->setMessageHandler(function (QueueMessage $message, Worker $worker) {
    // 一次处理200ms,这会造成消息量很多时,Dispatcher大量fork出worker进行处理
    usleep(200000);
});

$dispatcher = new Dispatcher([
    'host' => '127.0.0.1',
    'port' => 5672,
    'vhost' => '/',
    'user' => 'guest',
    'password' => 'guest',
], $factory);
// 这里将worker限制在一定量,这会降低消费速度,但是不会因为fork将系统资源耗完
$dispatcher->setMaxWorkers(50);
$dispatcher->connect(['queue1', 'queue2', 'queue3']);

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
    echo "Max Memory Usage: ".number_format($stat['memoryUsage']).' Bytes'.PHP_EOL;
    echo "Max Peak Memory Usage: ".number_format($stat['peakMemoryUsage']).' Bytes'.PHP_EOL;
});

$dispatcher->addSignalHandler(SIGINT, function () use ($dispatcher) {
    $dispatcher->shutdown();
});

echo ' [*] Waiting for messages. To exit press CTRL+C', "\n";
$dispatcher->run();