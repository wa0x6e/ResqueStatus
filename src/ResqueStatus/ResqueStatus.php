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

   /**
    * Active workers key.
    *
    * Used to store the started workers' runtime arguments (in a Redis hash).
    *
    * @var string
    * @see ResqueStatus::addWorker()
    * @see ResqueStatus::clearWorkers()
    * @see ResqueStatus::getWorkers()
    * @see ResqueStatus::isRunningSchedulerWorker()
    * @see ResqueStatus::removeWorker()
    */
    public static $workerKey = 'ResqueWorker';

   /**
    * Active scheduler worker key.
    *
    * Used to store the started scheduler worker PID (in a Redis string).
    *
    * @var string
    * @see ResqueStatus::isSchedulerWorker()
    * @see ResqueStatus::isRunningSchedulerWorker()
    * @see ResqueStatus::registerSchedulerWorker()
    * @see ResqueStatus::unregisterSchedulerWorker()
    */
    public static $schedulerWorkerKey = 'ResqueSchedulerWorker';

   /**
    * Paused workers key.
    *
    * Used to store the paused workers' names (in a Redis set).
    *
    * @var string
    * @see ResqueStatus::clearWorkers()
    * @see ResqueStatus::getPausedWorker()
    * @see ResqueStatus::setPausedWorker()
    */
    public static $pausedWorkerKey = 'PausedWorker';

    /**
     * Redis instance.
     *
     * @var Resqueredis|Redis
     */
    protected $redis = null;

    public function __construct($redis)
    {
        $this->redis = $redis;
    }

    /**
     * Save a worker's runtime arguments.
     *
     * Used when restarting the worker.
     *
     * @params int $pid Worker PID, e.g. 30677.
     * @param array $args Worker settings.
     * @return boolean True if successfull, false otherwise.
     */
    public function addWorker($pid, $args)
    {
        $hostname = $this->getHostname();
        $key = "$hostname:$pid";
        return $this->redis->hSet(self::$workerKey, $key, serialize($args)) !== false;
    }

    /**
     * Register a Scheduler Worker.
     *
     * @since 0.0.1
     * @params int $pid Worker PID, e.g. 30677.
     * @return boolean True if a Scheduler worker is found among the list of active workers.
     */
    public function registerSchedulerWorker($pid)
    {
        $hostname = $this->getHostname();
        $key = "$hostname:$pid";
        return $this->redis->set(self::$schedulerWorkerKey, $key);
    }

    /**
     * Test if a given worker is a scheduler worker.
     *
     * @since 0.0.1
     * @param Worker|string $worker Worker to test. If string it is the worker name, e.g. 'localhost:30677:default'.
     * @return boolean True if the worker is a scheduler worker.
     */
    public function isSchedulerWorker($worker)
    {
        list($host, $pid, ) = explode(':', (string)$worker);
        $key = "$host:$pid";
        return $key === $this->redis->get(self::$schedulerWorkerKey);
    }

    /**
     * Check if the Scheduler Worker is already running.
     *
     * Note: the scheduler only runs once, even across multiple hosts!
     *
     * @return boolean True if the scheduler worker is already running.
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
     * Unregister a Scheduler Worker.
     *
     * @since 0.0.1
     * @return boolean True if the scheduler worker existed and was successfully unregistered.
     */
    public function unregisterSchedulerWorker()
    {
        return $this->redis->del(self::$schedulerWorkerKey) > 0;
    }

    /**
     * Return all started workers' runtime arguments.
     *
     * @return array An array of workers' runtime arguments.
     */
    public function getWorkers()
    {
        $workers = $this->redis->hGetAll(self::$workerKey);
        $temp = array();

        $thisHostname = $this->getHostname();

        foreach ($workers as $name => $value) {
            [$host, $pid] = explode(':', $name);
            if ($thisHostname !== $host) {
                continue;
            }

            $temp[$pid] = unserialize($value);
        }

        return $temp;
    }

    /**
     * Remove a worker's runtime arguments.
     *
     * @params int $pid Worker PID, e.g. 30677.
     * @return void
     */
    public function removeWorker($pid)
    {
        $hostname = $this->getHostname();
        $key = "$hostname:$pid";
        $this->redis->hDel(self::$workerKey, $key);
    }

    /**
     * Clear all workers' runtime arguments.
     *
     * @return void
     */
    public function clearWorkers()
    {
        $this->redis->del(self::$workerKey);
        $this->redis->del(self::$pausedWorkerKey);
    }

    /**
     * Mark a worker as paused/active.
     *
     * @since 0.0.1
     * @param string $workerName Worker name, e.g. 'localhost:30677:default'.
     * @param boolean $paused Whether to mark the worker as paused or active.
     * @return void
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
     * Return a list of paused workers.
     *
     * @since 0.0.1
     * @return array An array of paused workers' name.
     */
    public function getPausedWorker()
    {
        return (array)$this->redis->smembers(self::$pausedWorkerKey);
    }

    /**
     * Returns the current hostname as to filter out unrelated workers/processes.
     *
     * @return string
     */
    private function getHostname()
    {
        if (function_exists('gethostname')) {
            return gethostname();
        } else {
            return php_uname('n');
        }
    }
}
