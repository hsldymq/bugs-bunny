<?php

use PHPUnit\Framework\TestCase;
use Archman\BugsBunny\WorkerScheduler;

class WorkerSchedulerTest extends TestCase
{
//    public function testAddWorker()
//    {
//        $scheduler = new WorkerScheduler(1);
//
//        $scheduler->add('a');
//        $this->assertCountWorker(1, 0, 0, $scheduler);
//        $this->assertHardCountWorker(1, 0, 0, $scheduler);
//
//        $scheduler->add('b');
//        $this->assertCountWorker(2, 0, 0, $scheduler);
//        $this->assertHardCountWorker(2, 0, 0, $scheduler);
//
//        // 重复增加没效果
//        $scheduler->add('a');
//        $this->assertCountWorker(2, 0, 0, $scheduler);
//        $this->assertHardCountWorker(2, 0, 0, $scheduler);
//    }
//
//    /**
//     * @depends testAddWorker
//     */
//    public function testRemoveWorker()
//    {
//        $scheduler = new WorkerScheduler(1);
//
//        $scheduler->add('a');
//        $scheduler->add('b');
//
//        $scheduler->remove('a');
//        $this->assertCountWorker(1, 0, 0, $scheduler);
//        $this->assertHardCountWorker(1, 0, 0, $scheduler);
//
//        // 重复移除无效果
//        $scheduler->remove('a');
//        $this->assertCountWorker(1, 0, 0, $scheduler);
//        $this->assertHardCountWorker(1, 0, 0, $scheduler);
//
//        // 移除不存在的worker无效果
//        $scheduler->remove('c');
//        $this->assertCountWorker(1, 0, 0, $scheduler);
//        $this->assertHardCountWorker(1, 0, 0, $scheduler);
//
//        $scheduler->remove('b');
//        $this->assertCountWorker(0, 0, 0, $scheduler);
//        $this->assertHardCountWorker(0, 0, 0, $scheduler);
//    }
//
//    /**
//     * @depends testAddWorker
//     */
//    public function testRetireWorker()
//    {
//        $scheduler = new WorkerScheduler(1);
//        $scheduler->add('a');
//        $scheduler->add('b');
//
//        $scheduler->retire('a');
//        $this->assertCountWorker(1, 1, 0, $scheduler);
//        $this->assertHardCountWorker(1, 1, 0, $scheduler);
//
//        // 重复退休没效果
//        $scheduler->retire('a');
//        $scheduler->retire('a');
//        $scheduler->retire('a');
//        $this->assertCountWorker(1, 1, 0, $scheduler);
//        $this->assertHardCountWorker(1, 1, 0, $scheduler);
//
//        // 退休不存在的worker没效果
//        $scheduler->retire('c');
//        $this->assertCountWorker(1, 1, 0, $scheduler);
//        $this->assertHardCountWorker(1, 1, 0, $scheduler);
//
//        $scheduler->retire('b');
//        $this->assertCountWorker(0, 2, 0, $scheduler);
//        $this->assertHardCountWorker(0, 2, 0, $scheduler);
//    }
//
//    /**
//     * @depends testRemoveWorker
//     * @depends testRetireWorker
//     */
//    public function testCombineBasicOperations()
//    {
//        $scheduler = new WorkerScheduler(5);
//        $scheduler->add('a');
//        $scheduler->add('b');
//        $scheduler->add('c');
//
//        $scheduler->remove('b');
//        $scheduler->remove('d');    // 不存在的worker
//        $scheduler->retire('c');
//        $scheduler->retire('e');    // 不存在的worker
//        $scheduler->add('f');
//
//        $this->assertCountWorker(2, 1, 0, $scheduler);
//        $this->assertHardCountWorker(2, 1, 0, $scheduler);
//    }
//
//    /**
//     * @depends testCombineBasicOperations
//     */
//    public function testAllocate()
//    {
//        $scheduler = new WorkerScheduler(2);
//        $scheduler->add('a');
//        $scheduler->add('b');
//        $scheduler->add('c');
//
//        $this->assertEquals('c', $scheduler->allocate());
//        $this->assertEquals('b', $scheduler->allocate());
//        $this->assertEquals('a', $scheduler->allocate());
//        $this->assertCountWorker(3, 0, 0, $scheduler);
//        $this->assertHardCountWorker(3, 0, 0, $scheduler);
//
//        $this->assertEquals('a', $scheduler->allocate());
//        $this->assertCountWorker(3, 0, 1, $scheduler);
//        $this->assertHardCountWorker(3, 0, 1, $scheduler);
//
//        $this->assertEquals('b', $scheduler->allocate());
//        $this->assertCountWorker(3, 0, 2, $scheduler);
//        $this->assertHardCountWorker(3, 0, 2, $scheduler);
//
//        $this->assertEquals('c', $scheduler->allocate());
//        $this->assertCountWorker(3, 0, 3, $scheduler);
//        $this->assertHardCountWorker(3, 0, 3, $scheduler);
//
//        $this->assertNull($scheduler->allocate());
//        $this->assertNull($scheduler->allocate());
//        $this->assertCountWorker(3, 0, 3, $scheduler);
//        $this->assertHardCountWorker(3, 0, 3, $scheduler);
//    }
//
//    /**
//     * @depends testAllocate
//     */
//    public function testRelease()
//    {
//        $scheduler = new WorkerScheduler(2);
//        $scheduler->add('a');
//        $scheduler->add('b');
//        $scheduler->add('c');
//        $scheduler->add('d');
//        $scheduler->add('e');
//
//        $scheduler->allocate();     // e: 1
//        $scheduler->allocate();     // d: 1
//        $scheduler->allocate();     // c: 1
//        $scheduler->allocate();     // b: 1
//        $scheduler->allocate();     // a: 1
//
//        $scheduler->allocate();     // a: 0
//        $scheduler->allocate();     // b: 0
//        $scheduler->allocate();     // c: 0
//
//        $scheduler->release('c');       // c: 1
//        $this->assertCountWorker(5, 0, 2, $scheduler);
//        $this->assertHardCountWorker(5, 0, 2, $scheduler);
//
//        $scheduler->release('a');       // a:1
//        $this->assertCountWorker(5, 0, 1, $scheduler);
//        $this->assertHardCountWorker(5, 0, 1, $scheduler);
//
//        $scheduler->retire('a');        // a:0:r
//        $scheduler->retire('d');        // d:0:r
//
//        $scheduler->release('b');       // b:1
//        $this->assertCountWorker(3, 2, 0, $scheduler);
//        $this->assertHardCountWorker(3, 2, 0, $scheduler);
//
//        // a:0:r
//        // b:1
//        // c:1
//        // d:0:r
//        // e:1
//
//        $scheduler->release('a');
//        $scheduler->release('b');
//        $scheduler->release('b');
//        $this->assertCountWorker(3, 2, 0, $scheduler);
//        $this->assertHardCountWorker(3, 2, 0, $scheduler);
//    }

    public function testChangeLevelHigher()
    {
        $scheduler = new WorkerScheduler(2);
        $scheduler->add('a');       // a:2
        $scheduler->add('b');       // b:2
        $scheduler->add('c');       // c:2
        $scheduler->add('d');       // d:2

        $scheduler->allocate();     // d:1
        $scheduler->allocate();     // c:1
        $scheduler->allocate();     // b:1
        $scheduler->allocate();     // a:1
        $scheduler->allocate();     // a:0
        $scheduler->retire('d');

        $scheduler->changeLevels(3);
        $scheduler->add('e');

        // a:1
        // b:2
        // c:2
        // d:0:r
        // e:3

        $this->assertCountWorker(4, 1, 0, $scheduler);
        $this->assertHardCountWorker(4, 1, 0, $scheduler);
    }

    public function testChangeLevelLower()
    {
        // TODO
    }

    private function assertCountWorker(int $expectWorking, int $expectRetired, int $expectBusy, WorkerScheduler $scheduler)
    {
        $this->assertEquals($expectWorking, $scheduler->countWorking());
        $this->assertEquals($expectRetired, $scheduler->countRetired());
        $this->assertEquals($expectBusy, $scheduler->countBusy());
    }

    private function assertHardCountWorker(int $expectWorking, int $expectRetired, int $expectBusy, WorkerScheduler $scheduler)
    {
        $working = (function () {
            $total = 0;
            foreach ($this->scheduleLevels as $level => $workers) {
                if ($level === 0) {
                    foreach ($workers as $_ => $state) {
                        $total += ($state === WorkerScheduler::WORKING ? 1 : 0);
                    }
                } else {
                    $total += count($workers);
                }
            }
            return $total;
        })->bindTo($scheduler, $scheduler)();

        $retired = (function () {
            $total = 0;
            foreach ($this->scheduleLevels[0] as $state) {
                $total += ($state === WorkerScheduler::RETIRED ? 1 : 0);
            }
            return $total;
        })->bindTo($scheduler, $scheduler)();

        $busy = (function () {
            $total = 0;
            foreach ($this->scheduleLevels[0] as $state) {
                $total += ($state === WorkerScheduler::WORKING ? 1 : 0);
            }
            return $total;
        })->bindTo($scheduler, $scheduler)();

        $this->assertEquals($expectWorking, $working);
        $this->assertEquals($expectRetired, $retired);
        $this->assertEquals($expectBusy, $busy);
    }
}