<?php

declare(strict_types=1);

namespace Archman\BugsBunny;

use Bunny\Async\Client;
use Bunny\ClientStateEnum;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use React\Promise;

/**
 * 在有些情况下,amqp断开连接时,并没有更新连接状态,导致无法正常退出.
 *
 * 使用这个类来覆盖原来的Async Client,做一个hack修复,期待之后bunny解决这个问题.
 *
 * @see https://github.com/jakubkulhan/bunny/issues/74
 */
class BunnyAsyncClient extends Client implements EventEmitterInterface
{
    use EventEmitterTrait;

    public function onDataAvailable(): void
    {
        try {
            parent::onDataAvailable();
        } catch (\Throwable $e) {
            $this->eventLoop->removeReadStream($this->getStream());
            $this->eventLoop->futureTick(function () use ($e) {
                $this->emit('error', [$e, $this]);
            });
        }
    }

    /**
     * @return Promise\PromiseInterface
     */
    public function connect(): Promise\PromiseInterface
    {
        $deferred = new Promise\Deferred();

        $errBack = function (\Throwable $e) use ($deferred, &$errBack) {
            $this->state = ClientStateEnum::ERROR;
            $this->removeListener('error', $errBack);
            $deferred->reject($e);
        };

        $this->on('error', $errBack);

        parent::connect()->then(
            function () use ($deferred) {
                $deferred->resolve($this);
            },
            function (\Throwable $e) use ($deferred) {
                // needed in case rejected not by the errBack
                $deferred->reject($e);
            }
        )->always(function () use ($errBack) {
            $this->removeListener('error', $errBack);
        });

        return $deferred->promise();
    }
}