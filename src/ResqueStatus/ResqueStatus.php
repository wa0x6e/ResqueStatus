<?php
/**
 * ResqueStatus File
 *
 * Saving the workers statuses
 *
 * PHP version 5
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @author        Wan Qi Chen <kami@kamisama.me>
 * @copyright     Copyright 2013, Wan Qi Chen <kami@kamisama.me>
 * @link          https://github.com/kamisama/ResqueStatus
 * @package       ResqueStatus
 * @since         0.0.1
 * @license       MIT License (http://www.opensource.org/licenses/mit-license.php)
 */

namespace ResqueStatus;

/**
 * ResqueStatus Class
 *
 * Saving the workers statuses
 */
class ResqueStatus
{

    public static $workerKey = 'ResqueWorker';

    public static $schedulerWorkerKey = 'ResqueSchedulerWorker';

    public static $pausedWorkerKey = 'PausedWorker';

    /**
     * Redis instance
     * @var Resqueredis|Redis
     */
    protected $redis = null;

    public function __construct($redis)
    {
        $this->redis = $redis;
    }

    /**
     * Save the workers arguments
     *
     * Used when restarting the worker
     *
     * @param  array $args Worker settings
     */
    public function addWorker($pid, $args)
    {
        return $this->redis->hSet(self::$workerKey, $pid, serialize($args)) !== false;
    }

    /**
     * Register a Scheduler Worker
     *
     * @since   0.0.1
     * @params  array   $workers    List of active workers
     * @return  boolean             True if a Scheduler worker is found among the list of active workers
     */
    public function registerSchedulerWorker($pid)
    {
        return $this->redis->set(self::$schedulerWorkerKey, $pid);
    }

    /**
     * Test if a given worker is a scheduler worker
     *
     * @since   0.0.1
     * @param   Worker|string   $worker Worker to test
     * @return  boolean                 True if the worker is a scheduler worker
     */
    public function isSchedulerWorker($worker)
    {
        list($host, $pid, $queue) = explode(':', (string)$worker);
        return $pid === $this->redis->get(self::$schedulerWorkerKey);
    }

    /**
     * Check if the Scheduler Worker is already running
     *
     * @return boolean        True if the scheduler worker is already running
     */
    public function isRunningSchedulerWorker()
    {
        $pids = $this->redis->hKeys(self::$workerKey);
        $schedulerPid = $this->redis->get(self::$schedulerWorkerKey);

        if ($schedulerPid !== false && is_array($pids)) {
            if (in_array($schedulerPid, $pids)) {
                return true;
            }
            // Pid is outdated, remove it
            $this->unregisterSchedulerWorker();
            return false;
        }
        return false;
    }

    /**
     * Unregister a Scheduler Worker
     *
     * @since  0.0.1
     * @return boolean True if the scheduler worker existed and was successfully unregistered
     */
    public function unregisterSchedulerWorker()
    {
        return $this->redis->del(self::$schedulerWorkerKey) > 0;
    }

    /**
     * Return all started workers arguments
     *
     * @return array An array of settings, by worker
     */
    public function getWorkers()
    {
        $workers = $this->redis->hGetAll(self::$workerKey);
        $temp = array();

        foreach ($workers as $name => $value) {
            $temp[$name] = unserialize($value);
        }
        return $temp;
    }

    /**
     *
     */
    public function removeWorker($pid)
    {
        $this->redis->hDel(self::$workerKey, $pid);
    }

    /**
     * Clear all workers saved arguments
     */
    public function clearWorkers()
    {
        $this->redis->del(self::$workerKey);
        $this->redis->del(self::$pausedWorkerKey);
    }

    /**
     * Mark a worker as paused/active
     *
     * @since 0.0.1
     * @param string    $workerName Name of the paused worker
     * @param bool      $paused     Whether to mark the worker as paused or active
     */
    public function setPausedWorker($workerName, $paused = true)
    {
        if ($paused) {
            $this->redis->sadd(self::$pausedWorkerKey, $workerName);
        } else {
            $this->redis->srem(self::$pausedWorkerKey, $workerName);
        }
    }

    /**
     * Return a list of paused workers
     *
     * @since 0.0.1
     * @return  array   An array of paused workers' name
     */
    public function getPausedWorker()
    {
        return (array)$this->redis->smembers(self::$pausedWorkerKey);
    }
}
