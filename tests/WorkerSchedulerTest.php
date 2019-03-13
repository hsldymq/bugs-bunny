<?php

use PHPUnit\Framework\TestCase;
use Archman\BugsBunny\WorkerScheduler;

class WorkerSchedulerTest extends TestCase
{
    public function testAddWorker()
    {
        $scheduler = new WorkerScheduler(1);

        $scheduler->add('a');
        $this->assertEquals(1, $scheduler->countWorking());
        $this->assertEquals(0, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

        $scheduler->add('b');
        $this->assertEquals(2, $scheduler->countWorking());
        $this->assertEquals(0, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

        // 重复增加没效果
        $scheduler->add('a');
        $this->assertEquals(2, $scheduler->countWorking());
        $this->assertEquals(0, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());
    }

    /**
     * @depends testAddWorker
     */
    public function testRemoveWorker()
    {
        $scheduler = new WorkerScheduler(1);

        $scheduler->add('a');
        $scheduler->add('b');

        $scheduler->remove('a');
        $this->assertEquals(1, $scheduler->countWorking());
        $this->assertEquals(0, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

        // 重复移除无效果
        $scheduler->remove('a');
        $this->assertEquals(1, $scheduler->countWorking());
        $this->assertEquals(0, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

        // 移除不存在的worker无效果
        $scheduler->remove('a');
        $this->assertEquals(1, $scheduler->countWorking());
        $this->assertEquals(0, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

        $scheduler->remove('b');
        $this->assertEquals(0, $scheduler->countWorking());
        $this->assertEquals(0, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

    }

    /**
     * @depends testAddWorker
     */
    public function testRetireWorker()
    {
        $scheduler = new WorkerScheduler(1);
        $scheduler->add('a');
        $scheduler->add('b');

        $scheduler->retire('a');
        $this->assertEquals(1, $scheduler->countWorking());
        $this->assertEquals(1, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

        // 重复退休没效果
        $scheduler->retire('a');
        $this->assertEquals(1, $scheduler->countWorking());
        $this->assertEquals(1, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

        // 退休不存在的worker没效果
        $scheduler->retire('c');
        $this->assertEquals(1, $scheduler->countWorking());
        $this->assertEquals(1, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());

        $scheduler->retire('b');
        $this->assertEquals(0, $scheduler->countWorking());
        $this->assertEquals(2, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());
    }

    /**
     * @depends testRemoveWorker
     * @depends testRetireWorker
     */
    public function testCombineBasicOperations()
    {
        $scheduler = new WorkerScheduler(5);
        $scheduler->add('a');
        $scheduler->add('b');
        $scheduler->add('c');

        $scheduler->remove('b');
        $scheduler->remove('d');    // 不存在的worker
        $scheduler->retire('c');
        $scheduler->retire('e');    // 不存在的worker
        $scheduler->add('f');

        $this->assertEquals(2, $scheduler->countWorking());
        $this->assertEquals(1, $scheduler->countRetired());
        $this->assertEquals(0, $scheduler->countBusy());
    }
}