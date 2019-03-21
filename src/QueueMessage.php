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
     * @var array
     */
    private $headers;

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
     *      'headers' => (array),
     *      'content' => (string),
     * ]
     */
    public function __construct(array $info)
    {
        $this->exchange = $this->checkOrGet($info, 'exchange');
        $this->queue = $this->checkOrGet($info, 'queue');
        $this->routingKey = $this->checkOrGet($info, 'routingKey');
        $this->headers = $this->checkOrGet($info, 'headers');
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
     * @return array
     */
    public function getHeaders(): array
    {
        return $this->headers;
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

    /**
     * @param array $info
     * @param string $field
     * @return mixed
     * @throws \Exception
     */
    private function checkOrGet(array $info, string $field)
    {
        if (!isset($info[$field])) {
            throw new \Exception("Lack of {$field} field.");
        }

        return $info[$field];
    }
}