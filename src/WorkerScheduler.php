<?php

namespace Archman\BugsBunny;

/**
 * Worker调度器.
 *
 * 存在多个worker进程的时候,要将新消息调度给哪个进程存在算法上的考虑.
 * 一种是轮询,将每个消息依次按照派发给每个worker. 用另一种是按照取最空闲的worker中最近一个处理过消息的worker.
 * 从数据结构来看,前者类似于循环链表,后者类似于栈.
 * 由于允许worker空闲退出的功能,所以前者无法实现这个要求,所以选择后者.
 */
class WorkerScheduler
{
    const WORKING = 1;

    const RETIRED = 2;

    /**
     * @var array
     * Dispatcher允许一个worker有一个队列同时发送多个消息给它,为了实现按照闲置情况来调度worker
     * 这里的数组结构包含worker队列上限+1个元素,每一个数组元素代表一个闲置等级.
     * 最高下标中代表最空闲,没有被调度的worker
     * 次高代表被调度出一次,一次类推
     * 下标0中为忙碌中或者已退休的worker.
     * [
     *      [
     *          $workerID => (integer),     // self::WORKING 允许调度, self::RETIRED 退休,停止调度
     *          ...
     *      ],
     *      ...
     * ]
     */
    private $scheduleLevels = [];

    /**
     * @var array
     * [
     *      $workerID => $level,
     *      ...
     * ]
     */
    private $levelMap = [];

    /**
     * @var int 已退休的worker数量
     */
    private $retiredNum = 0;

    /**
     * @var int 未退休的worker数量(包含忙碌中的worker)
     */
    private $workingNum = 0;

    /**
     * @var int 忙碌中不参与调度的worker数量
     */
    private $busyNum = 0;

    /**
     * @param int $levels must be great then 0
     */
    public function __construct(int $levels)
    {
        $this->changeLevels($levels);
    }

    /**
     * 将一个worker加入到调度器中.
     *
     * @param string $workerID
     * @param bool $allocated
     */
    public function add(string $workerID, bool $allocated = false)
    {
        if (isset($this->levelMap[$workerID])) {
            return;
        }

        if ($allocated) {
            $level = count($this->scheduleLevels) - 2;
        } else {
            $level = count($this->scheduleLevels) - 1;
        }
        $this->scheduleLevels[$level][$workerID] = self::WORKING;
        $this->levelMap[$workerID] = $level;
        $this->increase('working');
        if ($level === 0) {
            $this->increase('busy');
        }
    }

    /**
     * 从调度器中移除worker.
     *
     * @param string $workerID
     */
    public function remove(string $workerID)
    {
        $level = $this->levelMap[$workerID] ?? null;
        if ($level === null) {
            return;
        }

        if ($this->scheduleLevels[$level][$workerID] === self::RETIRED) {
            $this->decrease('retired');
        } else {
            $this->decrease('working');
            $level === 0 && $this->decrease('busy');
        }

        unset($this->scheduleLevels[$level][$workerID]);
        unset($this->levelMap[$workerID]);
    }

    /**
     * 将一个worker置为退休,该worker不再参与调度.
     *
     * @param string $workerID
     */
    public function retire(string $workerID)
    {
        $level = $this->levelMap[$workerID] ?? null;
        if ($level === null) {
            return;
        }

        $state = $this->scheduleLevels[$level][$workerID];
        if ($state !== self::RETIRED) {
            $this->levelMap[$workerID] = 0;
            $this->scheduleLevels[0][$workerID] = self::RETIRED;

            $this->increase('retired');
            $this->decrease('working');
            if ($level === 0) {
                $this->decrease('busy');
            }

            unset($this->scheduleLevels[$level][$workerID]);
        }
    }

    /**
     * 分配一个可以用的worker.
     *
     * 分配后,该worker的调度等级减1.
     *
     * @return string|null 成功分配返回worker id, 没有可用的worker返回null
     */
    public function allocate()
    {
        $workerID = null;
        for ($i = count($this->scheduleLevels) - 1; $i > 0; $i--) {
            $state = end($this->scheduleLevels[$i]);
            if (!$state) {
                continue;
            }
            do {
                if ($state !== self::RETIRED) {
                    $workerID = key($this->scheduleLevels[$i]);

                    // 讲worker的调度等级降低一级
                    $this->scheduleLevels[$i - 1][$workerID] = $state;
                    $this->levelMap[$workerID] = $i - 1;
                    unset($this->scheduleLevels[$i][$workerID]);

                    if ($i - 1 === 0) {
                        $this->increase('busy');
                    }

                    break 2;
                }
            } while ($state = prev($this->levelMap[$i]));
        }

        return $workerID;
    }

    /**
     * 归还一个worker.
     *
     * 该worker的调度等级加1.
     *
     * @param string $workerID
     */
    public function release(string $workerID)
    {
        $level = $this->levelMap[$workerID] ?? null;

        if ($level === null || $this->scheduleLevels[$level][$workerID] === self::RETIRED) {
            return;
        }

        $newLevel = min(count($this->scheduleLevels) - 1, $level + 1);
        if ($level === $newLevel) {
            return;
        }

        $this->scheduleLevels[$newLevel][$workerID] = true;
        $this->levelMap[$workerID] = $newLevel;
        unset($this->scheduleLevels[$level][$workerID]);

        if ($level === 0) {
            $this->decrease('busy');
        }
    }

    /**
     * 变更调度等级数.
     *
     * 调整后的等级数大于或小于原等级数,那么存在两种情况
     * 新的等级数更大时,原来处于忙碌的worker就不再处于忙碌状态,于是可以进行调度
     * 更小时,原来不忙碌的worker可能就会处于忙碌状态,可以调度的worker就变少了.
     *
     * @param int $newLevelNum
     */
    public function changeLevels(int $newLevelNum)
    {
        $oldLevelNum = count($this->scheduleLevels);
        if ($oldLevelNum === $newLevelNum + 1 || $newLevelNum <= 0) {
            return;
        }

        $newScheduleLevels = array_fill(0, $newLevelNum + 1, []);
        if ($oldLevelNum === 0) {
            $this->scheduleLevels = $newScheduleLevels;
            return;
        }

        $diff = $newLevelNum + 1 - $oldLevelNum;
        foreach ($this->scheduleLevels as $level => $workers) {
            foreach ($workers as $workerID => $state) {
                if ($state === self::RETIRED) {
                    $newScheduleLevels[0][$workerID] = $state;
                    $this->levelMap[$workerID] = 0;
                } else {
                    if ($diff > 0) {
                        $newLevel = min($level + $diff, $newLevelNum);
                    } else {
                        // $diff < 0
                        $newLevel = max(0, $level + $diff);
                    }
                    $newScheduleLevels[$newLevel][$workerID] = $state;
                    $this->levelMap[$workerID] = $newLevel;

                    if ($level > 0 && $newLevel === 0) {
                        $this->increase('busy');
                    } else if ($level === 0 && $newLevel > 0) {
                        $this->decrease('busy');
                    }
                }
            }
        }
        $this->scheduleLevels = $newScheduleLevels;
    }

    /**
     * 返回剩余可调度的worker.
     *
     * @return int
     */
    public function countSchedulable(): int
    {
        return $this->countWorking() - $this->countBusy();
    }

    /**
     * @return int 返回未退休的worker数量(包含忙碌中的worker).
     */
    public function countWorking(): int
    {
        return $this->workingNum;
    }

    /**
     * @return int 返回忙碌中的worker数量, 忙碌中的worker是不参与调度的.
     */
    public function countBusy(): int
    {
        return $this->busyNum;
    }

    /**
     * @return int 返回已退休的worker数量.
     */
    public function countRetired(): int
    {
        return $this->retiredNum;
    }

    private function increase(string $which)
    {
        switch ($which) {
            case 'working':
                $this->workingNum++;
                break;
            case 'busy':
                $this->busyNum++;
                break;
            case 'retired':
                $this->retiredNum++;
                break;
        }
    }

    private function decrease(string $which)
    {
        switch ($which) {
            case 'working':
                $this->workingNum--;
                break;
            case 'busy':
                $this->busyNum--;
                break;
            case 'retired':
                $this->retiredNum--;
                break;
        }
    }
}