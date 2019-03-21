<?php

/**
 * 这个脚本用于投放消息到rabbitmq队列中,供后续的示例脚本消费.
 * 修改amqp_params.php中的连接配置来使得你的机器连上自己的或远程的rabbitmq服务器
 *
 * 执行命令:
 *      php 00.publish_msg.php ${消息数量} ${每条消息大小} ${是否每秒周期性持续投放}
 *      其中参数3值为0或1,并且可选, 默认为0
 */

require_once __DIR__.'/../vendor/autoload.php';

use Bunny\Channel;
use Bunny\Client;
use Archman\Whisper\AbstractMaster;

(new class () extends AbstractMaster {
    /**
     * @var int
     */
    private $numMessages;

    /**
     * @var int
     */
    private $contentLength;

    /**
     * @var bool
     */
    private $periodic;

    /**
     * @var Client
     */
    private $client;

    /**
     * @var Channel
     */
    private $channel;

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
        $params = require __DIR__.'/amqp_params.php';
        $queues = $params['queues'];
        $exchange = $params['exchange'];

        $this->client = new Client($params['connectionOptions']);
        $this->client->connect();
        $this->channel = $this->client->channel();
        $this->channel->exchangeDeclare($exchange);
        foreach ($queues as $routingKey => $queue) {
            $this->channel->queueDeclare($queue);
            $this->channel->queueBind($queue, $exchange, $routingKey);
        }

        if ($this->periodic) {
            $this->addSignalHandler(SIGINT, function () {
                $this->stopProcess();
            });
            $this->addTimer(1, true, function () use ($queues, $exchange) {
                $routingKeys = array_keys($queues);
                for ($i = 0; $i < $this->numMessages; $i++) {
                    $key = $routingKeys[array_rand($routingKeys)];
                    $content = str_repeat('x', $this->contentLength);
                    $this->channel->publish($content, [], $exchange, $key);
                }
            });
            $this->process();
        } else {
            $routingKeys = array_keys($queues);
            for ($i = 0; $i < $this->numMessages; $i++) {
                $key = $routingKeys[array_rand($routingKeys)];
                $content = str_repeat('x', $this->contentLength);
                $this->channel->publish($content, [], $exchange, $key);
            }
        }

        echo "done!\n";
    }

    public function onMessage(string $workerID, \Archman\Whisper\Message $msg)
    {}
})->run();
