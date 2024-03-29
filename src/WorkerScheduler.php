<?php

declare(strict_types=1);

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
    public const WORKING = 1;

    public const RETIRED = 2;

    /**
     * @var array
     * 下标0中为忙碌中或者已退休的worker.
     * 下标1中为空闲的worker.
     * [
     *      0 => [
     *          $workerID => (integer),     // 枚举值: self::WORKING 忙碌中,闲置后允许调度, self::RETIRED 退休,停止调度
     *          ...
     *      ],
     *      1 => [
     *          $workerID => (integer),     // 枚举值: self::WORKING 闲置中,允许调度
     *          ...
     *      ],
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

    public function __construct()
    {
        $this->changeLevels(1);
    }

    /**
     * 将一个worker加入到调度器中.
     *
     * @param string $workerID
     * @param bool $allocated 是否已经被分配,true时自动将它的分配等级降1
     */
    public function add(string $workerID, bool $allocated = false): void
    {
        if (isset($this->levelMap[$workerID])) {
            return;
        }

        if ($allocated) {
            $level = count($this->scheduleLevels) - 2;
        } else {
            $level = count($this->scheduleLevels) - 1;
        }
        $this->setWorkerLevel($workerID, $level, self::WORKING);
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
    public function remove(string $workerID): void
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
    public function retire(string $workerID): void
    {
        $level = $this->levelMap[$workerID] ?? null;
        if ($level === null) {
            return;
        }

        $state = $this->scheduleLevels[$level][$workerID];
        if ($state !== self::RETIRED) {
            $this->setWorkerLevel($workerID, 0, self::RETIRED, $level);
            $this->increase('retired');
            $this->decrease('working');
            if ($level === 0) {
                $this->decrease('busy');
            }
        }
    }

    /**
     * 分配一个可以用的worker.
     *
     * 分配后,该worker的调度等级减1.
     *
     * @return string|null 成功分配返回worker id, 没有可用的worker返回null
     */
    public function allocate(): ?string
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
                    $this->setWorkerLevel($workerID, $i - 1, self::WORKING, $i);
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
    public function release(string $workerID): void
    {
        $level = $this->levelMap[$workerID] ?? null;

        if ($level === null || $this->scheduleLevels[$level][$workerID] === self::RETIRED) {
            return;
        }

        $newLevel = min(count($this->scheduleLevels) - 1, $level + 1);
        if ($level === $newLevel) {
            return;
        }

        $this->setWorkerLevel($workerID, $newLevel, self::WORKING, $level);
        if ($level === 0) {
            $this->decrease('busy');
        }
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

    /**
     * 变更调度等级数.
     *
     * 调整后的等级数大于或小于原等级数,那么存在两种情况
     * 新的等级数更大时,原来处于忙碌的worker就不再处于忙碌状态,于是可以进行调度
     * 更小时,原来不忙碌的worker可能就会处于忙碌状态,可以调度的worker就变少了.
     *
     * @param int $newLevelNum
     *
     * @return self
     */
    public function changeLevels(int $newLevelNum): self
    {
        $oldLevelNum = count($this->scheduleLevels);
        if ($oldLevelNum === $newLevelNum + 1 || $newLevelNum <= 0) {
            return $this;
        }

        $newScheduleLevels = array_fill(0, $newLevelNum + 1, []);
        if ($oldLevelNum === 0) {
            $this->scheduleLevels = $newScheduleLevels;
            return $this;
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

        return $this;
    }

    private function increase(string $which): void
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

    private function decrease(string $which): void
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

    /**
     * @param string $workerID
     * @param int $newLevel
     * @param int $state
     * @param int|null $oldLevel
     *
     * @return WorkerScheduler
     */
    private function setWorkerLevel(string $workerID, int $newLevel, int $state, int $oldLevel = null): self
    {
        $this->scheduleLevels[$newLevel][$workerID] = $state;
        $this->levelMap[$workerID] = $newLevel;
        if ($oldLevel !== null && $newLevel !== $oldLevel) {
            unset($this->scheduleLevels[$oldLevel][$workerID]);
        }

        return $this;
    }
}