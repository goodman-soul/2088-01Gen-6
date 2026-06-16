import threading
import time
import signal
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable
from datetime import datetime, timedelta


class TaskState(Enum):
    RUNNING = "running"
    LOST_CONTACT = "lost_contact"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FORCE_STOPPED = "force_stopped"


@dataclass
class TaskInfo:
    task_id: str
    name: str
    state: TaskState = TaskState.RUNNING
    last_heartbeat: Optional[datetime] = None
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    exit_code: Optional[int] = None
    heartbeat_interval: float = 2.0
    timeout: float = 6.0
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: Optional[threading.Thread] = None
    work_fn: Optional[Callable] = None


class TaskWorker:
    _counter = 0
    _counter_lock = threading.Lock()

    def __init__(
        self,
        name: str,
        work_fn: Callable[[threading.Event, Callable], int],
        heartbeat_interval: float = 2.0,
        timeout: float = 6.0,
    ):
        with TaskWorker._counter_lock:
            TaskWorker._counter += 1
            self._id = f"task-{TaskWorker._counter}"

        self._info = TaskInfo(
            task_id=self._id,
            name=name,
            heartbeat_interval=heartbeat_interval,
            timeout=timeout,
            work_fn=work_fn,
        )
        self._heartbeat_callback: Optional[Callable[[str], None]] = None

    @property
    def info(self) -> TaskInfo:
        return self._info

    @property
    def task_id(self) -> str:
        return self._id

    def set_heartbeat_callback(self, callback: Callable[[str], None]):
        self._heartbeat_callback = callback

    def _send_heartbeat(self):
        if self._heartbeat_callback:
            self._heartbeat_callback(self._id)

    def start(self):
        self._info.start_time = datetime.now()
        self._info.state = TaskState.RUNNING
        self._info.stop_event.clear()

        def _run():
            try:
                exit_code = self._info.work_fn(self._info.stop_event, self._send_heartbeat)
                self._info.exit_code = exit_code if exit_code is not None else 0
            except Exception as e:
                self._info.exit_code = 1
            finally:
                self._info.end_time = datetime.now()
                if self._info.state != TaskState.LOST_CONTACT:
                    self._info.state = TaskState.STOPPED

        self._info.thread = threading.Thread(target=_run, name=self._id, daemon=True)
        self._info.thread.start()

    def request_stop(self):
        if self._info.state == TaskState.RUNNING:
            self._info.state = TaskState.STOPPING
            self._info.stop_event.set()
        elif self._info.state == TaskState.LOST_CONTACT:
            self._info.stop_event.set()

    def is_alive(self) -> bool:
        return self._info.thread is not None and self._info.thread.is_alive()

    def join(self, timeout: float = 5.0):
        if self._info.thread is not None:
            self._info.thread.join(timeout=timeout)


class HeartbeatMonitor:
    def __init__(
        self,
        check_interval: float = 1.0,
        default_timeout: float = 6.0,
    ):
        self._check_interval = check_interval
        self._default_timeout = default_timeout
        self._tasks: dict[str, TaskWorker] = {}
        self._lock = threading.Lock()
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def register(self, worker: TaskWorker):
        worker.set_heartbeat_callback(self._on_heartbeat)
        with self._lock:
            self._tasks[worker.task_id] = worker

    def _on_heartbeat(self, task_id: str):
        with self._lock:
            worker = self._tasks.get(task_id)
            if worker:
                worker.info.last_heartbeat = datetime.now()
                if worker.info.state == TaskState.LOST_CONTACT:
                    worker.info.state = TaskState.RUNNING

    def start(self):
        self._running = True
        self._stop_event.clear()

        def _monitor_loop():
            while not self._stop_event.is_set():
                self._check_tasks()
                self._stop_event.wait(self._check_interval)

        self._monitor_thread = threading.Thread(
            target=_monitor_loop, name="heartbeat-monitor", daemon=True
        )
        self._monitor_thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)

        for worker in self._tasks.values():
            worker.request_stop()

        for worker in self._tasks.values():
            info = worker.info
            if info.state in (TaskState.STOPPED, TaskState.FORCE_STOPPED):
                continue
            worker.join(timeout=5.0)
            if not worker.is_alive():
                if info.state != TaskState.STOPPED:
                    info.state = TaskState.STOPPED
                    if info.end_time is None:
                        info.end_time = datetime.now()
                    if info.exit_code is None:
                        info.exit_code = -1
            else:
                info.state = TaskState.FORCE_STOPPED
                if info.end_time is None:
                    info.end_time = datetime.now()
                if info.exit_code is None:
                    info.exit_code = -9

    def _check_tasks(self):
        now = datetime.now()
        with self._lock:
            for worker in self._tasks.values():
                info = worker.info
                if info.state in (TaskState.STOPPED, TaskState.FORCE_STOPPED):
                    continue

                if info.state == TaskState.STOPPING:
                    if not worker.is_alive():
                        info.state = TaskState.STOPPED
                        if info.end_time is None:
                            info.end_time = now
                        if info.exit_code is None:
                            info.exit_code = -1
                    continue

                if info.state == TaskState.LOST_CONTACT:
                    if not worker.is_alive():
                        info.state = TaskState.STOPPED
                        if info.end_time is None:
                            info.end_time = now
                        if info.exit_code is None:
                            info.exit_code = -1
                    continue

                if info.state == TaskState.RUNNING:
                    if info.last_heartbeat is not None:
                        elapsed = (now - info.last_heartbeat).total_seconds()
                        if elapsed > info.timeout:
                            info.state = TaskState.LOST_CONTACT
                            print(
                                f"[MONITOR] ⚠ 任务 '{info.name}' ({info.task_id}) "
                                f"心跳超时 ({elapsed:.1f}s > {info.timeout:.1f}s)，标记为失联"
                            )
                            worker.request_stop()
                    else:
                        elapsed = (now - info.start_time).total_seconds()
                        if elapsed > info.timeout:
                            info.state = TaskState.LOST_CONTACT
                            print(
                                f"[MONITOR] ⚠ 任务 '{info.name}' ({info.task_id}) "
                                f"从未发送心跳，标记为失联"
                            )
                            worker.request_stop()

    def generate_report(self) -> str:
        now = datetime.now()
        state_names = {
            TaskState.RUNNING: "运行中",
            TaskState.LOST_CONTACT: "失联",
            TaskState.STOPPING: "停止中",
            TaskState.STOPPED: "已停止",
            TaskState.FORCE_STOPPED: "强制停止",
        }
        lines = []
        lines.append("=" * 72)
        lines.append("  任务心跳监控报告")
        lines.append(f"  生成时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 72)
        lines.append("")

        header = f"{'任务ID':<10} {'名称':<16} {'状态':<10} {'最近心跳':<20} {'运行时长':<14} {'退出码':<8}"
        lines.append(header)
        lines.append("-" * 72)

        with self._lock:
            for worker in self._tasks.values():
                info = worker.info

                state_str = state_names.get(info.state, info.state.value)

                if info.last_heartbeat:
                    last_hb_str = info.last_heartbeat.strftime("%H:%M:%S")
                else:
                    last_hb_str = "从未响应"

                if info.end_time:
                    duration = info.end_time - info.start_time
                else:
                    duration = now - info.start_time

                total_seconds = int(duration.total_seconds())
                hours, remainder = divmod(total_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                duration_str = f"{hours}h{minutes:02d}m{seconds:02d}s"

                if info.exit_code is not None:
                    exit_str = str(info.exit_code)
                else:
                    exit_str = "-"

                line = f"{info.task_id:<10} {info.name:<16} {state_str:<10} {last_hb_str:<20} {duration_str:<14} {exit_str:<8}"
                lines.append(line)

        lines.append("")
        lines.append("=" * 72)

        running = sum(1 for w in self._tasks.values() if w.info.state == TaskState.RUNNING)
        lost = sum(1 for w in self._tasks.values() if w.info.state == TaskState.LOST_CONTACT)
        stopping = sum(1 for w in self._tasks.values() if w.info.state == TaskState.STOPPING)
        stopped = sum(1 for w in self._tasks.values() if w.info.state == TaskState.STOPPED)
        force_stopped = sum(1 for w in self._tasks.values() if w.info.state == TaskState.FORCE_STOPPED)

        lines.append(
            f"  汇总: 运行中={running}  失联={lost}  停止中={stopping}  "
            f"已停止={stopped}  强制停止={force_stopped}"
        )
        lines.append("=" * 72)

        return "\n".join(lines)


def healthy_worker(stop_event: threading.Event, heartbeat: Callable) -> int:
    """正常工作任务：持续发送心跳直到被停止"""
    count = 0
    while not stop_event.is_set():
        heartbeat()
        count += 1
        stop_event.wait(2.0)
    return 0


def slow_heartbeat_worker(stop_event: threading.Event, heartbeat: Callable) -> int:
    """慢心跳任务：心跳间隔较长但仍在超时范围内"""
    while not stop_event.is_set():
        heartbeat()
        stop_event.wait(4.5)
    return 0


def flaky_worker(stop_event: threading.Event, heartbeat: Callable) -> int:
    """不稳定任务：发送几次心跳后停止发送（模拟卡死/失联）"""
    for _ in range(3):
        if stop_event.is_set():
            return 1
        heartbeat()
        stop_event.wait(2.0)
    while not stop_event.is_set():
        stop_event.wait(1.0)
    return 1


def quick_exit_worker(stop_event: threading.Event, heartbeat: Callable) -> int:
    """快速退出任务：工作几秒后自行退出"""
    for _ in range(2):
        if stop_event.is_set():
            return 0
        heartbeat()
        stop_event.wait(1.0)
    return 0


def error_exit_worker(stop_event: threading.Event, heartbeat: Callable) -> int:
    """错误退出任务：工作后以非零退出码退出"""
    for _ in range(2):
        if stop_event.is_set():
            return 2
        heartbeat()
        stop_event.wait(1.0)
    return 2


def silent_worker(stop_event: threading.Event, heartbeat: Callable) -> int:
    """沉默任务：从不发送心跳（模拟启动后卡死）"""
    while not stop_event.is_set():
        stop_event.wait(1.0)
    return 0


def stubborn_worker(stop_event: threading.Event, heartbeat: Callable) -> int:
    """顽固任务：忽略停止信号，一直运行（测试强制停止）"""
    count = 0
    while True:
        heartbeat()
        count += 1
        for _ in range(20):
            if stop_event.is_set():
                pass
            time.sleep(0.1)
    return 0


def main():
    monitor = HeartbeatMonitor(check_interval=0.5, default_timeout=3.0)

    workers = [
        TaskWorker("数据采集", healthy_worker, heartbeat_interval=1.0, timeout=3.0),
        TaskWorker("日志处理", healthy_worker, heartbeat_interval=1.0, timeout=3.0),
        TaskWorker("慢速同步", slow_heartbeat_worker, heartbeat_interval=2.5, timeout=5.0),
        TaskWorker("不稳定节点", flaky_worker, heartbeat_interval=1.0, timeout=3.5),
        TaskWorker("沉默僵尸", silent_worker, heartbeat_interval=1.0, timeout=3.0),
        TaskWorker("顽固服务", stubborn_worker, heartbeat_interval=0.5, timeout=3.0),
        TaskWorker("健康检查", quick_exit_worker, heartbeat_interval=1.0, timeout=3.0),
        TaskWorker("异常服务", error_exit_worker, heartbeat_interval=1.0, timeout=3.0),
    ]

    print("[MAIN] 启动心跳监控器...")
    monitor.start()

    print("[MAIN] 启动工作任务...")
    for w in workers:
        monitor.register(w)
        w.start()
        print(f"[MAIN]   已启动: {w.info.name} ({w.task_id})")

    def _signal_handler(signum, frame):
        print("\n[MAIN] 收到终止信号，正在关闭...")
        monitor.stop()
        print(monitor.generate_report())
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    report_interval = 4.0
    last_report_time = time.monotonic()
    total_run_time = 12.0
    start = time.monotonic()

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= total_run_time:
            break

        now = time.monotonic()
        if now - last_report_time >= report_interval:
            print(monitor.generate_report())
            last_report_time = now

        time.sleep(0.5)

    print("\n[MAIN] 运行结束，正在关闭所有任务...")
    monitor.stop()

    print("\n[MAIN] 最终报告:")
    print(monitor.generate_report())


if __name__ == "__main__":
    main()
