<?php

require_once __DIR__.'/../vendor/autoload.php';

use Bunny\Channel;
use Bunny\Client;
use Archman\Whisper\AbstractMaster;

(new class () extends AbstractMaster {
    private $numMessages;

    private $contentLength;

    private $periodic;

    private $connectionOptions = [
        'host' => '127.0.0.1',
        'port' => 5672,
        'vhost' => '/',
        'user' => 'guest',
        'password' => 'guest',
    ];

    /**
     * @var Client
     */
    private $client;

    /**
     * @var Channel
     */
    private $channel;

    private $exchange = "bugsbunny";

    private $queues = [
        'queue1' => 'queue1',
        'queue2' => 'queue2',
        'queue3' => 'queue3',
    ];

    public function __construct()
    {
        parent::__construct();

        global $argv;

        if (count($argv) < 3) {
            echo "Usage: php 00.publish_msg.php 10 1000 0\n";
            echo "\t10: number of message.\n";
            echo "\t1000: content length(bytes).\n";
            echo "\t0: periodic(per second), 1:true, 0:false. this is optional.";
            exit();
        }

        $this->numMessages = intval($argv[1]);
        $this->contentLength = intval($argv[2]);
        $this->periodic = !!intval($argv[3] ?? false);
    }

    public function run()
    {
        $this->client = new Client($this->connectionOptions);
        $this->client->connect();
        $this->channel = $this->client->channel();
        $this->channel->exchangeDeclare($this->exchange);
        foreach ($this->queues as $routingKey => $queue) {
            $this->channel->queueDeclare($queue);
            $this->channel->queueBind($queue, $this->exchange, $routingKey);
        }

        if ($this->periodic) {
            $this->addSignalHandler(SIGINT, function () {
                $this->stopProcess();
            });
            $this->addTimer(1, true, function () {
                $routingKeys = array_keys($this->queues);
                for ($i = 0; $i < $this->numMessages; $i++) {
                    $key = $routingKeys[array_rand($routingKeys)];
                    $content = str_repeat('x', $this->contentLength);
                    $this->channel->publish($content, [], $this->exchange, $key);
                }
            });
            $this->process();
        } else {
            $routingKeys = array_keys($this->queues);
            for ($i = 0; $i < $this->numMessages; $i++) {
                $key = $routingKeys[array_rand($routingKeys)];
                $content = str_repeat('x', $this->contentLength);
                $this->channel->publish($content, [], $this->exchange, $key);
            }
        }

        echo "done!\n";
    }

    public function onMessage(string $workerID, \Archman\Whisper\Message $msg)
    {}
})->run();
