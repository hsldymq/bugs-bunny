<?php

namespace Archman\BugsBunny;

class QueueMessage
{
    /**
     * @var string
     */
    private $exchange;

    /**
     * @var string
     */
    private $queue;

    /**
     * @var string
     */
    private $routingKey;

    /**
     * @var string
     */
    private $content;

    /**
     * @param array $info
     * [
     *      'exchange' => (string),
     *      'queue' => (string),
     *      'routingKey' => (string),
     *      'content' => (string),
     * ]
     */
    public function __construct(array $info)
    {
        $this->exchange = $this->checkOrGet($info, 'exchange');
        $this->queue = $this->checkOrGet($info, 'queue');
        $this->routingKey = $this->checkOrGet($info, 'routingKey');
        $this->content = $this->checkOrGet($info, 'content');
    }

    /**
     * @return string
     */
    public function getContent(): string
    {
        return $this->content;
    }

    /**
     * @return string
     */
    public function getExchange(): string
    {
        return $this->exchange;
    }

    /**
     * @return string
     */
    public function getQueue(): string
    {
        return $this->queue;
    }

    /**
     * @return string
     */
    public function getRoutingKey(): string
    {
        return $this->routingKey;
    }

    private function checkOrGet(array $info, string $field): string
    {
        if (!isset($info[$field])) {
            throw new \Exception("Lack of {$field} field.");
        }

        return $info[$field];
    }
}