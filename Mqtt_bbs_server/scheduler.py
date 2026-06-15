"""
BBScheduler — 基于 BBS 的定时任务调度器

对标 reflect/scheduler.py 的轮询调度，改为通过 BBS 发布任务，
Worker 按能力认领执行。

任务 JSON 格式（sche_tasks/*.json）:
    {
        "schedule": "08:00",
        "repeat": "daily",
        "enabled": true,
        "prompt": "...",
        "max_delay_hours": 6,
        "target_capability": "ops"     // 可选，按能力路由
    }

用法:
    from Mqtt_bbs_server.scheduler import BBScheduler
    sched = BBScheduler()
    sched.start()       # 前台阻塞运行
"""

import os, json, time as _time, logging
from datetime import datetime, timedelta
from typing import Optional

from .bbs import AgentBoard

log = logging.getLogger("Mqtt_bbs.scheduler")

# ── 路径 (与 reflect/scheduler.py 保持一致) ──
_dir = os.path.dirname(os.path.abspath(__file__))
TASKS = os.path.join(_dir, '..', 'sche_tasks')
DONE  = os.path.join(_dir, '..', 'sche_tasks', 'done')
DEFAULT_MAX_DELAY = 6


def _parse_cooldown(repeat: str) -> timedelta:
    """解析 repeat 为冷却时间"""
    if repeat == 'once':
        return timedelta(days=999999)
    if repeat in ('daily', 'weekday'):
        return timedelta(hours=20)
    if repeat == 'weekly':
        return timedelta(days=6)
    if repeat == 'monthly':
        return timedelta(days=27)
    if repeat.startswith('every_'):
        try:
            parts = repeat.split('_')
            n = int(parts[1].rstrip('hdm'))
            u = parts[1][-1]
            if u == 'h':
                return timedelta(hours=n)
            if u == 'm':
                return timedelta(minutes=n)
            if u == 'd':
                return timedelta(days=n)
        except (ValueError, IndexError):
            pass
        log.warning(f'Unknown repeat type: {repeat}, fallback to 20h cooldown')
    return timedelta(hours=20)


def _last_run(tid: str, done_files: set) -> Optional[datetime]:
    """找最近一次执行时间"""
    latest = None
    for df in done_files:
        if not df.endswith(f'_{tid}.md'):
            continue
        try:
            t = datetime.strptime(df[:15], '%Y-%m-%d_%H%M')
            if latest is None or t > latest:
                latest = t
        except ValueError:
            continue
    return latest


class BBScheduler:
    """
    BBS 定时调度器

    独立进程运行，轮询 sche_tasks/*.json 并将满足条件的任务
    通过 BBS 发布给 Worker 执行。
    """

    def __init__(self, agent_id: str = "bbs_scheduler",
                 poll_interval: int = 60):
        self.agent_id = agent_id
        self.poll_interval = poll_interval
        self._board: Optional[AgentBoard] = None
        self._running = False
        self._last_l4_archive = 0.0

    # ── 生命周期 ──

    def start(self):
        """启动调度器（前台阻塞）"""
        # 连接 BBS
        self._board = AgentBoard(self.agent_id)
        self._board._client.connect()
        self._board._client.wait_connected(5)
        log.info(f"[{self.agent_id}] [START] BBScheduler 启动 (poll={self.poll_interval}s)")

        self._running = True
        os.makedirs(DONE, exist_ok=True)

        try:
            while self._running:
                self._tick()
                _time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """停止调度器"""
        self._running = False
        if self._board:
            self._board._client.disconnect()
        log.info(f"[{self.agent_id}] BBScheduler 停止")

    def _tick(self):
        """单次轮询"""
        now = datetime.now()
        done_files = set(os.listdir(DONE)) if os.path.isdir(DONE) else set()

        # L4 归档（每12h）
        self._check_l4_archive(now)

        # 扫描任务
        if not os.path.isdir(TASKS):
            return

        for f in sorted(os.listdir(TASKS)):
            if not f.endswith('.json'):
                continue
            tid = f[:-5]
            task = self._load_task(os.path.join(TASKS, f))
            if not task:
                continue

            if not self._should_trigger(task, tid, now, done_files):
                continue

            # 触发！
            self._trigger_task(task, tid, now)

    def _load_task(self, path: str) -> Optional[dict]:
        """加载并验证任务 JSON"""
        try:
            with open(path, encoding='utf-8') as fp:
                return json.loads(fp.read())
        except Exception as e:
            log.error(f'JSON parse error for {os.path.basename(path)}: {e}')
            return None

    def _should_trigger(self, task: dict, tid: str,
                        now: datetime, done_files: set) -> bool:
        """判断是否应该触发此任务"""
        if not task.get('enabled', False):
            return False

        repeat = task.get('repeat', 'daily')
        sched = task.get('schedule', '00:00')
        try:
            h, m = map(int, sched.split(':'))
        except (ValueError, TypeError):
            log.error(f'Invalid schedule in {tid}: {sched!r}')
            return False

        # weekday 任务周末跳过
        if repeat == 'weekday' and now.weekday() >= 5:
            return False

        # 还没到 schedule 时间
        if now.hour < h or (now.hour == h and now.minute < m):
            return False

        # 执行窗口检查
        max_delay = task.get('max_delay_hours', DEFAULT_MAX_DELAY)
        sched_minutes = h * 60 + m
        now_minutes = now.hour * 60 + now.minute
        if (now_minutes - sched_minutes) > max_delay * 60:
            log.info(f'SKIP {tid}: {now_minutes - sched_minutes}min past schedule')
            return False

        # 冷却检查
        last = _last_run(tid, done_files)
        cooldown = _parse_cooldown(repeat)
        if last and (now - last) < cooldown:
            return False

        return True

    def _trigger_task(self, task: dict, tid: str, now: datetime):
        """通过 BBS 发布任务"""
        prompt = task.get('prompt', '')
        target_cap = task.get('target_capability', None)

        log.info(f'TRIGGER {tid} (repeat={task.get("repeat")}, '
                 f'schedule={task.get("schedule")}, cap={target_cap})')

        # BBS 发布任务
        try:
            if self._board:
                task_id = self._board.post_task_routed(
                    task_type=tid,
                    task_input={"prompt": prompt, "source": "scheduler"},
                    target_capability=target_cap,
                )
                log.info(f'  [OK] BBS 任务已发布: {task_id}')
            else:
                log.warning(f'  [WARN] BBS 不可用，跳过 {tid}')
        except Exception as e:
            log.error(f'  [FAIL] BBS 发布失败: {e}')

    def _check_l4_archive(self, now: datetime):
        """每12小时 L4 归档"""
        if _time.time() - self._last_l4_archive > 43200:
            self._last_l4_archive = _time.time()
            try:
                import sys
                sys.path.insert(0, os.path.join(_dir, '..', 'GA', 'memory', 'L4_raw_sessions'))
                from compress_session import batch_process  # type: ignore
                raw_dir = os.path.join(_dir, '..', 'GA', 'temp', 'model_responses')
                r = batch_process(raw_dir, dry_run=False)
                log.info(f'[L4 cron] {r}')
            except Exception as e:
                log.error(f'L4 archive failed: {e}')


# ── 命令行入口 ──

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    sched = BBScheduler()
    sched.start()


if __name__ == "__main__":
    main()
