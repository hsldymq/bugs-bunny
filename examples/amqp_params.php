<?php

return [
    'connectionOptions' => [
        'host' => '127.0.0.1',
        'port' => 5672,
        'vhost' => '/',
        'user' => 'guest',
        'password' => 'guest',
    ],
    'exchange' => 'bugsbunny',
    'queues' => [
        // $routingKey => $queue
        'queue1' => 'queue1',
        'queue2' => 'queue2',
        'queue3' => 'queue3',
    ],
];