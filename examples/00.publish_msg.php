<?php

require_once __DIR__.'/../vendor/autoload.php';

use Bunny\Client;

if (count($argv) < 3) {
    echo "Usage: php 00.publish_msg.php 10 1000\n";
    echo "\t10: number of message.\n";
    echo "\t1000: content length(bytes).\n";
    exit();
}

$exchange = 'bugsbunny';
$queues = [
    'queue1' => 'queue1',
    'queue2' => 'queue2',
    'queue3' => 'queue3',
];

$client = new Client([
    'host' => '127.0.0.1',
    'port' => 5672,
    'vhost' => '/',
    'user' => 'guest',
    'password' => 'guest',
]);
$client->connect();
$channel = $client->channel();
$channel->exchangeDeclare($exchange);
foreach ($queues as $routingKey => $queue) {
    $channel->queueDeclare($queue);
    $channel->queueBind($queue, $exchange, $routingKey);
}


$numMsg = intval($argv[1]);
$routingKeys = array_keys($queues);
for ($i = 0; $i < $numMsg; $i++) {
    $key = $routingKeys[array_rand($routingKeys)];

    $content = str_repeat('x', intval($argv[2]));
    $channel->publish($content, [], $exchange, $key);
}

echo "done!\n";
