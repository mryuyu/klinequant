"""backtest_pump — 多策略回测的确定性 lockstep 回放泵

需求：多策略回测共享**单一模拟账户 + 单一时间轴**（组合资金曲线），且回放
结果可复现（同输入两次跑一致）。策略函数各自持有 ``while api.wait_update()``
阻塞循环，无法在单线程内交错，故用「主线程泵 + 每策略一个门（gate）+ 工作线程」
实现确定性 lockstep：

  主线程（泵）持有真实 BacktestDataFeed（全策略 symbols×periods 并集的统一时间轴）。
  每步：
    ① real_feed.wait_and_snapshot() 推进一个收盘事件（触发 executor.mark_equity）；
    ② 把 (step, 快照) publish 给每个策略的 gate；
    ③ 等待**全体** gate ack（= 各策略已处理完本步、阻塞在下一步门口）才推进。

  策略线程的 ``GatedFeed.wait_and_snapshot`` 阻塞取事件、返回主线程的快照；读操作
  （latest_bars/tick/now_ms/version）全部委托真实 feed——泵在等 ack 期间不推进时间轴，
  故多线程并发读安全、且所有策略看到同一时刻的数据（无 look-ahead、无竞态）。

ack 采用「下一步门口回执」语义（off-by-one 消除）：策略在**进入下一次** wait 时
才 ack 上一步已处理完，故泵收到 stepN 的 ack 时 stepN 的所有下单/记账都已落定，
才推进到 stepN+1。执行顺序确定 → 结果可复现。
"""
from __future__ import annotations

import logging
import queue
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = ["Gate", "GatedFeed", "run_lockstep"]

# 终止哨兵：publish 后策略 next_step 取到即返回 (False, {})，策略循环自然退出
_TERMINATE = None


class Gate:
    """单策略 lockstep 门（主线程 publish/wait_ack ↔ 策略线程 next_step）。

    线程安全：两个 queue 各自单向（主→策略 事件；策略→主 ack）。
    """

    def __init__(self, name: str = ""):
        self.name = name
        self._events: "queue.Queue[Optional[Tuple[int, dict]]]" = queue.Queue()
        self._acks: "queue.Queue[bool]" = queue.Queue()
        self._started = False
        self._terminated = False

    # ─── 主线程侧 ───

    def publish(self, step: int, snap: Dict[str, int]) -> None:
        """推送一个时间轴事件给策略线程。"""
        self._events.put((step, snap))

    def wait_ack(self) -> None:
        """阻塞等待策略处理完当前步（进入下一步门口回执）。"""
        self._acks.get()

    def terminate(self) -> None:
        """数据耗尽：推送终止哨兵，策略 next_step 返回 False 退出循环。"""
        self._events.put(_TERMINATE)

    # ─── 策略线程侧 ───

    def next_step(self) -> Tuple[bool, Dict[str, int]]:
        """策略线程取下一个事件（阻塞）。返回 (是否推进, 推进前版本快照)。

        首次调用不 ack（无上一步）；此后每次调用先 ack 上一步已处理完，再阻塞取事件。
        幂等终止：一旦取到终止哨兵即置 ``_terminated``，后续调用**立即**返回
        ``(False, {})``（不再 ack、不阻塞）——使策略提前退出（break/resilient 放弃）后
        的 drain 循环能安全收尾，避免泵 ``wait_ack`` / ``join`` 死锁。
        """
        if self._terminated:
            return False, {}
        if self._started:
            self._acks.put(True)   # 上一步处理完毕的回执
        self._started = True
        item = self._events.get()
        if item is _TERMINATE:
            self._terminated = True
            return False, {}
        step, snap = item
        return True, snap

    def drain(self) -> None:
        """策略提前退出（break/resilient 放弃）后继续消费事件直到终止。

        持续 ack + 取事件，使泵不会因本 gate 不再 ack 而卡在 ``wait_ack``；
        已终止时 ``next_step`` 立即返 False，本方法瞬间返回（幂等安全）。
        """
        while self.next_step()[0]:
            pass


class GatedFeed:
    """DataFeedProtocol 包装：wait_* 经 gate 同步，其余读操作委托真实 feed。

    每个策略一个实例（共享同一个真实 feed）。KqApi 拿到的是本包装，故其
    wait_update 走 gate lockstep；latest_bars/current_version 等直读真实 feed
    （泵等 ack 期间时间轴静止，读安全）。
    """

    def __init__(self, real_feed: Any, gate: Gate):
        self._real = real_feed
        self._gate = gate

    # ─── 门控：流程控制 ───

    def wait_and_snapshot(
        self, deadline: float | None = None, symbol: str | None = None,
    ) -> tuple[bool, dict]:
        return self._gate.next_step()

    def wait_update(self, deadline: Optional[float] = None) -> bool:
        got, _snap = self._gate.next_step()
        return got

    # ─── 委托：读操作（时间轴静止期并发安全）───

    def latest_tick(self, symbol: str):
        return self._real.latest_tick(symbol)

    def latest_bars(self, symbol: str, period: str, count: int = 200) -> List[dict]:
        return self._real.latest_bars(symbol, period, count)

    def resolve_key(self, obj: Any, field: Optional[str] = None):
        return self._real.resolve_key(obj, field)

    def current_version(self, key: str) -> int:
        return self._real.current_version(key)

    def is_changing(self, obj: Any, field: Optional[str] = None) -> bool:
        return self._real.is_changing(obj, field)

    def now_ms(self) -> int:
        return self._real.now_ms()

    def seconds_since_update(self, symbol: Optional[str] = None) -> float:
        return 0.0   # 回测无断线

    # ─── 委托：执行器取价（executor 直用真实 feed，此处为兼容透传）───

    @property
    def index(self) -> int:
        return self._real.index

    @property
    def n_bars(self) -> int:
        return self._real.n_bars

    def open_price(self, symbol: str):
        return self._real.open_price(symbol)

    def close_price(self, symbol: str):
        return self._real.close_price(symbol)


def run_lockstep(
    real_feed: Any,
    gates: List[Gate],
    workers: List[Tuple[Callable[[], None], str]],
) -> None:
    """驱动确定性 lockstep 回放（阻塞，直到时间轴耗尽且全体策略线程退出）。

    Args:
        real_feed: 真实 BacktestDataFeed（统一时间轴；其 on_bar 已绑 executor.mark_equity）
        gates: 每策略一个 Gate（与 workers 一一对应）
        workers: [(策略线程体 callable, 线程名), ...]；每个 callable 无参、内部跑
                 ``while api.wait_update(): ...``（api.feed 为对应 GatedFeed）

    流程：起工作线程 → 泵逐步推进（publish→wait_ack 全体）→ 耗尽后 terminate → join。
    任一策略线程异常退出（resilient 已兜底续跑，此处再兜一层）不阻塞泵：其 gate
    不再 ack 时泵会卡在 wait_ack，故 workers 线程体必须自行 catch-all（见调用方）。
    """
    threads: List[threading.Thread] = []
    for fn, name in workers:
        t = threading.Thread(target=fn, name=name, daemon=True)
        threads.append(t)
        t.start()

    step = -1
    try:
        while True:
            got, snap = real_feed.wait_and_snapshot()
            if not got:
                break
            step += 1
            for gate in gates:
                gate.publish(step, snap)
            for gate in gates:
                gate.wait_ack()
    finally:
        # 无论正常耗尽还是泵异常，都给所有 gate 发终止哨兵，避免策略线程永久阻塞
        for gate in gates:
            gate.terminate()
        for t in threads:
            t.join()
    logger.info(f"[PUMP] lockstep done: {step + 1} step(s), {len(workers)} strategy(ies)")
