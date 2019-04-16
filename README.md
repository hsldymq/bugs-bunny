# Bugs Bunny: 弹性的多进程RabbitMQ Consumer

### 运行要求
* PHP >= 7.0
* ext-pcntl
* ext-posix
* [可选,建议使用] ext-event / ext-ev / ext-uv

### 安装
> composer require hsldymq/bugs-bunny

### 这是什么?
Bugs Bunny是一个弹性的多进程RabbitMQ消费者库,它基于[bunny/bunny](https://github.com/jakubkulhan/bunny),[reactphp/event-loop](https://github.com/reactphp/event-loop),[hsldymq/whisper](https://github.com/hsldymq/whisper).

它提供一种能力,能够根据队列消息数量的多少动态地调整消费者数量,从而降低因单个消费者处理能力低下而导致队列消息阻塞不能及时处理;也因为其动态的特性在消息数量少的情况,主动减少消费者数量降低无意义的系统开销.

### 为什么要使用这个库而不是现存的其他方案?
目前绝大多数方案都是基于进程阻塞消费,即当有消息时候执行处理函数,没有消息的时候会持续阻塞等待.

当然像[php-amqplib/php-amqplib](https://github.com/php-amqplib/php-amqplib)这样的库是提供同步非阻塞的方式运行,但绝不是一个理想的方式,没人愿意在循环中空跑浪费资源.

于是这带来了一些问题:

* 一个新的业务消息你如何预估需要的消费者数量?
* 当一次活动或一个突发事件导致消息暴增,如何快速调整消费者数量来应对消息堆积的情况?
* 没有消息的时候,持续阻塞无异于浪费资源,我们是否可以关闭这个消费者,关闭后那么如果万一又有消息到来了呢?
* 当我们要关闭消费者,如何保证关掉的时候它没有正在处理消息? 换句话说,我们不希望业务代码被执行到一半给中断掉.

其实这些问题你都不用再关心, Bugs Bunny就是为了解决这些问题.

### 如何解决的?
不同于常见的阻塞方案,Bugs Bunny使用master/worker进程的方式,master进程(也叫做dispatcher)负责消费,调度worker进程,并将消息分配给worker进程,worker则只负责处理消息.

dispatcher连接到AMQP服务器,创建的worker进程也通过socket pair进行进程间通讯,所有连接的文件描述符都通过reachphp/event-loop库进行IO复用

这就解决了消费者数量无法动态调整的问题:

worker不关心消息源,只关心消息本身.有消息就处理,没消息就等待或这退出

dispatcher监听两方消息,由AMQP服务器发来的队列消息,dispatcher将它派发给闲置的worker,没有空闲就创建一个.同时他也接受来自worker的空闲和退出通知,用来调整的它的派发策略,并管理worker的安全退出.

### 示例
```php

use Archman\BugsBunny\QueueMessage;
use Archman\BugsBunny\WorkerFactory;
use Archman\BugsBunny\Dispatcher;
use Archman\BugsBunny\Worker;
use Archman\BugsBunny\Connection;

// worker工厂,定义了worker创建后的处理逻辑
$factory = (new WorkerFactory())
    // 设置当worker空闲n秒后会不再接收消息,并在处理完所有剩余消息后安全退出
    // 如果不设置这个,worker永远不会在闲置时退出
    ->setIdleShutdown($nSec)
    // 注册消息处理器,定义worker进程对消息的操作逻辑
    ->setMessageHandler(function (QueueMessage $message, Worker $worker) {
        $queue = $message->getQueue();
        $routingKey = $message->getRoutingKey();
        
        // 根据消息的种类执行响应的业务逻辑
    })
    ->registerEvent('start', function (Worker $worker) {
        // worker进程启动事件,用于进行一些初始化操作
    })
    ->registerEvent('error', function (string $reason, \Throwable $e, Worker $worker) {
        // 处理错误,打日志或其他操作,比如你还可以使用$worker->shutdown();使进程安全退出
    });

$factory->registerSignal(SIGINT, function () {
    // 可以注册信号处理器,防止worker被信号杀死
    // 我们只希望worker的启动关闭都是由dispatcher控制,这样才是安全的
})    


// 到AMQP服务器连接配置
$options = getAMQPConnectionOptions(); 
// 从配置中得到所有要消费的队列
$queues = getAllQueues();
$conn = new Connection($options, $queues);

$dispatcher = (new Dispatcher($conn, $factory))
    // 限制worker进程上限
    // 当消息量非常大而所有worker都处理不过来而时,你不会希望worker进程无止境的被创建的
    // 如果你假定不存在这种问题,你可以不设置它
    ->setMaxWorkers($numWorkers)
    // 设置缓存消息数量
    // 当worker数量达到上限,我们可以通过缓存一些消息来提高消息派发效率
    ->setCacheLimit(1000)
    ->on('start', function (Dispatcher $dispatcher) {
        // dispatcher开始运行,可以进行一些初始化操作
    })
    ->on('error', function (string $reason, \Throwable $e, Dispatcher $dispatcher) {
        // dispatcher发生一些错误
    })
    ->on('shutdown', function (Dispatcher $dispatcher) {
        // dispatcher即将退出
    });
$dispatcher->addSignalHandler(SIGINT, function () use ($dispatcher) {
    // 可以注册信号处理器,防止因为信号被杀死
    // 这里我们可以调用安全关闭方法.
    $dispatcher->shutdown();
});

// 你可以不传参数,但如果传true则是以daemon运行
$dispatcher->run(true);
```

### 更多
你可以通过查看examples目录中的代码来了解更多的信息