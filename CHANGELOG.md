# CHANGELOG

* 0.1.4  (2019-03-29 Asia/Chongqing)
    * 修复当AMQP服务器主动关闭连接而无法正常退出

* 0.1.3  (2019-03-28 Asia/Chongqing)
    * 允许Dispatcher以daemon进程运行
    * 修复在事件循环期间持续触发异常而无法正常退出时,因为无休止的设置timer导致内存占用达到php限制
    * 修复因绑定的队列不存在时无法正常退出,永远处于事件循环中

* 0.1.2  (2019-03-26 Asia/Chongqing)
    * 移除对psr/logger的依赖,日志记录由使用者自行调用

* 0.1.1  (2019-03-25 Asia/Chongqing)
    * Dispatcher增加一个事件(patrolling),在事件循环周期跳出时触发