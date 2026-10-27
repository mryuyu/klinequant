//+------------------------------------------------------------------+
//| AutoHistoryDownload.mq5                                          |
//| KlineQuant 配套 EA：挂到 MT5 任意图表即自动下载历史数据          |
//|                                                                  |
//| 原理：MQL5 的 HistorySelect() 与 CopyRates() 在本地缺数据时会    |
//| 自动向服务器发起历史下载（Python MetaTrader5 包没有这个触发入口，|
//| 只会被动读取终端已同步的部分）。EA 按品种 × 周期分片向历史深钻， |
//| 终端收到数据后缓存在数据目录，Python 端 copy_rates_* 即可读到。  |
//|                                                                  |
//| EA 模式说明：                                                    |
//|  - 挂上图表即自动开始下载，无需每次手工运行脚本；                |
//|  - 下载循环按"品种×周期"切片，每个 Timer tick 只处理一片，       |
//|    不阻塞终端事件线程；                                          |
//|  - 每个品种×周期开钻前先做一次深度探测（ProbeFirst）：终端缓存已 |
//|    触底则直接跳过，因此反复挂载/重启终端不会重复拉历史；          |
//|  - 全部跑完打印 DONE 后默认保留在图表上空转（已无动作，重启终端 |
//|    自动重挂也只会探测秒过）；若希望跑完自动从图表卸载，把       |
//|    AutoDetach 置 true（借图表重载实现，MQL5 无直接移除 EA 的 API，|
//|    图表会短暂刷新一次；模板里的 EA 会重挂但探测秒过不再重下）。  |
//|  - Sleep() 只能在 OnTimer/自定义函数里用（OnTick 不允许），本 EA |
//|    完全由 Timer 驱动，OnTick 空实现；单片轮询最长约 Retries×    |
//|    PauseMs（默认 12s），此间 Timer 事件排队不会重叠执行。        |
//|                                                                  |
//| 用法：                                                           |
//|  1. 把本文件复制到 MT5 数据目录 MQL5\Experts\ 下                |
//|     （MT5 菜单 文件→打开数据文件夹）                             |
//|  2. MetaEditor 打开本文件 → 编译（F7）                           |
//|  3. MT5 导航器 → 专家顾问 → 拖到任意图表，顶栏"算法交易"需开启   |
//|     （想开机自动跑：把该图表另存为模板并设为启动图表即可）        |
//|  4. 查看"工具 → 日志"(Ctrl+Shift+G) 输出进度；跑完显示 DONE      |
//|  品种范围：自动枚举市场观察全部品种，逐个 × 逐周期下载；         |
//|  可用 SymbolsFilter 前缀过滤（如 "EUR"/"XAU"）分批跑。           |
//|  注意：终端 设置→图表 里"最大 K 线数"建议调大（默认 500 万），   |
//|  否则超出 TERMINAL_MAXBARS 的历史不会下载。                      |
//+------------------------------------------------------------------+
#property copyright "KlineQuant"
#property version   "2.00"

//--- 输入参数
input int      MaxYears      = 10;      // 最大回溯年数（超出则停在源尽头）
input string   Timeframes    = "M1,M5,M15,M30,H1,H4,D1,W1"; // 需要下载的周期（逗号分隔）
input string   SymbolsFilter = "";      // 品种名前缀过滤（空=市场观察全部品种）
input int      PauseMs       = 300;     // 每次轮询间隔毫秒（给后台同步留时间；稳定会提前结束）
input int      Retries       = 40;      // 单片同步未稳定的最大轮询次数（约 40×300ms=12s 上限）
input bool     ProbeFirst    = true;    // 启动先探测缓存深度，已够深则不重复下载
input bool     AutoDetach    = false;   // 跑完自动从图表卸载（刷一次图表；模板内 EA 需手工移）

//--- 下载状态（跨 Timer tick 保持，按 品种→周期→分片 游标推进）
string    g_syms[];                     // 待下载品种
string    g_tfs[];                      // 周期名（与 g_tf_int 一一对应）
int       g_tf_int[];                   // 周期 ENUM 值
int       g_sym_n  = 0, g_sym_i = 0;    // 品种总数 / 当前品种索引
int       g_tf_n   = 0, g_tf_i  = 0;    // 周期总数 / 当前周期索引
datetime  g_cursor     = 0;             // 当前周期深钻游标
datetime  g_floor      = 0;             // 回溯起点
long      g_bars_total = 0;             // 当前品种×周期累计根数
int       g_empty      = 0;             // 连续空片计数
bool      g_started    = false;         // 首个 tick 已初始化
bool      g_done       = false;         // 全部完成 / 无需下载（空转）
bool      g_pending_dl = false;         // InitDownload 成功但尚未开跑
bool      g_probe      = false;         // 当前品种×周期待深度探测
int       g_last_sym   = -1, g_last_tf = -1; // 上次 Comment 刷新时的进度位置

//--- 周期字符串 -> ENUM（新增周期在此扩表）
ENUM_TIMEFRAMES TfFromStr(string tf)
{
   if(tf == "M1")  return PERIOD_M1;
   if(tf == "M2")  return PERIOD_M2;
   if(tf == "M5")  return PERIOD_M5;
   if(tf == "M15") return PERIOD_M15;
   if(tf == "M30") return PERIOD_M30;
   if(tf == "H1")  return PERIOD_H1;
   if(tf == "H2")  return PERIOD_H2;
   if(tf == "H4")  return PERIOD_H4;
   if(tf == "H12") return PERIOD_H12;
   if(tf == "D1")  return PERIOD_D1;
   if(tf == "W1")  return PERIOD_W1;
   if(tf == "MN1") return PERIOD_MN1;
   return PERIOD_CURRENT; // 未知写法回退当前图表周期
}

//--- 周期秒数（分片覆盖判定用）
int TfSeconds(ENUM_TIMEFRAMES tf)
{
   switch((int)tf)
   {
      case PERIOD_M1:  return 60;
      case PERIOD_M2:  return 120;
      case PERIOD_M5:  return 300;
      case PERIOD_M15: return 900;
      case PERIOD_M30: return 1800;
      case PERIOD_H1:  return 3600;
      case PERIOD_H2:  return 7200;
      case PERIOD_H4:  return 14400;
      case PERIOD_H12: return 43200;
      case PERIOD_D1:  return 86400;
      case PERIOD_W1:  return 604800;
      case PERIOD_MN1: return 2592000; // 近似 30 天
   }
   return 0;
}

//--- 单片周期分片跨度：日内周期约 720 根/片；日线以上 60 根/片（防单片请求过大）
int StepFor(ENUM_TIMEFRAMES tf)
{
   int secs = TfSeconds(tf);
   return (secs >= 86400) ? secs * 60 : secs * 720;
}

//+------------------------------------------------------------------+
//| 深度探测（当前品种×周期）：回溯起点处的最老缓存 bar 是否已触底 |
//| 够深 → true（跳过本周期）；不够/无数据 → false（开始深钻，此调用 |
//| 顺便把最深一片先拉下来）。按组合逐个探测，不在 OnInit 里循环阻塞 |
//+------------------------------------------------------------------+
bool ProbeCurrentPair()
{
   string          sym     = g_syms[g_sym_i];
   string          tfname  = g_tfs[g_tf_i];
   ENUM_TIMEFRAMES tf      = (ENUM_TIMEFRAMES)g_tf_int[g_tf_i];
   int             span    = 3 * StepFor(tf);
   datetime        probe_from = g_floor;
   datetime        probe_to   = probe_from + span;
   if(probe_to > TimeCurrent()) probe_to = TimeCurrent();

   MqlRates rates[];
   int copied = CopyRates(sym, tf, probe_from, probe_to, rates);
   bool deep = false;
   if(copied >= 2 && rates[0].time <= g_floor + span / 2)
      deep = true;                        // 起点窗内有多根且贴近起点 → 已触底
   if(deep)
      PrintFormat("[%s] %s 探测：缓存已触底（起点附近 %d 根），跳过下载",
                  sym, tfname, copied);
   return deep;
}

//+------------------------------------------------------------------+
//| 初始化：枚举市场观察全部品种，建下载队列                         |
//| 返回 false = 无匹配品种或数据已够深，无需启动 Timer              |
//+------------------------------------------------------------------+
bool InitDownload()
{
   g_floor = TimeCurrent() - (datetime)((long)MaxYears * 365 * 86400);

   //--- 周期表
   string tfs[];
   int cnt = StringSplit(Timeframes, ',', tfs);
   ArrayResize(g_tfs, 0);
   ArrayResize(g_tf_int, 0);
   for(int f = 0; f < cnt; f++)
   {
      string tfname = tfs[f];
      StringTrimLeft(tfname);
      StringTrimRight(tfname);
      if(tfname == "") continue;
      ENUM_TIMEFRAMES tf = TfFromStr(tfname);
      if(tf == PERIOD_CURRENT)
      {
         Print("未知周期 '", tfname, "'，跳过");
         continue;
      }
      int idx = ArraySize(g_tfs);
      ArrayResize(g_tfs, idx + 1);
      ArrayResize(g_tf_int, idx + 1);
      g_tfs[idx]    = tfname;
      g_tf_int[idx] = (int)tf;
   }
   g_tf_n = ArraySize(g_tfs);
   if(g_tf_n == 0)
   {
      Print("Timeframes 无有效周期，EA 停止");
      return false;
   }

   //--- 品种枚举（SymbolsTotal(true)=仅市场观察选中显示的那些）
   ArrayResize(g_syms, 0);
   int total = SymbolsTotal(true);
   for(int i = 0; i < total; i++)
   {
      string s = SymbolName(i, true);
      if(s == "") continue;
      if(SymbolsFilter != "" && StringFind(s, SymbolsFilter) != 0) continue; // 前缀过滤
      int idx = ArraySize(g_syms);
      ArrayResize(g_syms, idx + 1);
      g_syms[idx] = s;
   }
   g_sym_n = ArraySize(g_syms);
   if(g_sym_n == 0)
   {
      Print("市场观察无匹配品种（SymbolsFilter=\"", SymbolsFilter, "\"）");
      return false;
   }

   g_sym_i = 0; g_tf_i = 0; g_empty = 0; g_bars_total = 0;
   g_probe = ProbeFirst;               // 首个 tick 先探测，已够深则逐组合直接跳过

   SymbolSelect(g_syms[0], true);       // 已在市场观察仍显式确认一次，防部分服务器不响应历史
   g_cursor = TimeCurrent();

   PrintFormat("=== AutoHistoryDownload EA start: 市场观察 %d 个品种%s 范围 %s ~ %s 周期 %s",
               g_sym_n,
               (SymbolsFilter != "" ? "（前缀过滤 \"" + SymbolsFilter + "\"）" : ""),
               TimeToString(g_floor, TIME_DATE), TimeToString(TimeCurrent(), TIME_DATE),
               Timeframes);
   PrintFormat("--- (1/%d) %s ---", g_sym_n, g_syms[0]);
   return true;
}

//+------------------------------------------------------------------+
//| 处理一个分片：触发服务器同步 → 轮询到"根数稳定" → 游标向历史推进 |
//| 返回 true = 当前品种×周期已到达数据尽头（连续空片或触底）        |
//+------------------------------------------------------------------+
bool ProcessOneSlice()
{
   string sym    = g_syms[g_sym_i];
   string tfname = g_tfs[g_tf_i];
   ENUM_TIMEFRAMES tf = (ENUM_TIMEFRAMES)g_tf_int[g_tf_i];
   int    secs = TfSeconds(tf);
   datetime to = g_cursor;
   datetime from = to - StepFor(tf);
   if(from < g_floor) from = g_floor;

   //--- 触发下载并轮询到"根数稳定"（HistorySelect 后台异步，未下完时
   //    Bars/CopyRates 只返回部分 → 必须等稳定再判，否则浅层即被误杀）
   int      have = 0, prev = -1, stable = 0;
   datetime oldest = to;
   for(int r = 0; r < Retries; r++)
   {
      HistorySelect(from, to);            // 缺数据时向服务器发起历史请求
      int b = Bars(sym, tf, from, to);    // 区间内已同步的 bar 数
      if(b > 0)
      {
         MqlRates rates[];
         int copied = CopyRates(sym, tf, from, to, rates);  // 兜底补拉一次同步
         if(copied > 0)
         {
            b      = copied;
            oldest = rates[0].time;       // CopyRates 按时间升序，[0] 最老
         }
      }
      have = b;
      if(have > 0 && have == prev) stable++; else stable = 0;
      prev = have;
      if(have > 0 && stable >= 2) break;  // 连续两次相同 → 视为同步完成
      Sleep((uint)PauseMs);               // EA/指标可用 Sleep（脚本才需空转计时）
   }

   int expected = (int)((to - from) / secs);
   if(expected < 1) expected = 1;
   PrintFormat("[%s] %s 片 %s ~ %s 已取得 %d 根（期望≤%d），累计 %I64d",
               sym, tfname, TimeToString(from, TIME_DATE|TIME_MINUTES),
               TimeToString(to, TIME_DATE|TIME_MINUTES),
               have, expected, g_bars_total + have);

   const int GAP = secs * 12;            // 容忍假/长节假日的空档（按 bar 时长折算）

   if(have == 0)
   {
      if(++g_empty >= 4)
      {
         PrintFormat("[%s] %s 到达数据尽头（连续空片），完成：同步约 %I64d 根",
                     sym, tfname, g_bars_total);
         return true;
      }
      g_cursor = from - 1;               // 空片可能是停摆区/未就绪，跳过继续往深
      return false;
   }
   g_empty = 0;
   g_bars_total += have;

   // 从本片最老 bar 之前继续深钻；真尽头由 empty_streak 判定。
   // 用时间覆盖(而非根数比例)记录是否铺满，仅用于日志，不再据此 break，
   // 避免周末/节假日导致 D1/W1 等被误判尽头。
   if(oldest > from + GAP)
      PrintFormat("[%s] %s 本片起点到 %s 才起（可能有停摆/节假日空档，继续）",
                  sym, tfname, TimeToString(oldest, TIME_DATE|TIME_MINUTES));
   g_cursor = oldest - 1;                 // 铺到当前已同步的最深处 → 再往前

   if(g_cursor <= g_floor)
   {
      PrintFormat("[%s] %s 完成：同步约 %I64d 根", sym, tfname, g_bars_total);
      return true;
   }
   return false;
}

//+------------------------------------------------------------------+
//| 状态机推进：一个品种×周期跑完后移到下一周期/下一品种（重置探测） |
//+------------------------------------------------------------------+
void AdvanceState()
{
   g_tf_i++;
   if(g_tf_i >= g_tf_n)                   // 当前品种全部周期跑完
   {
      PrintFormat("--- (%d/%d) %s 全部周期完成 ---", g_sym_i + 1, g_sym_n, g_syms[g_sym_i]);
      g_sym_i++;
      g_tf_i = 0;
   }
   if(g_sym_i >= g_sym_n) { FinishAll(); return; }

   // 进入下一品种：刷新游标与计数
   SymbolSelect(g_syms[g_sym_i], true);
   PrintFormat("--- (%d/%d) %s ---", g_sym_i + 1, g_sym_n, g_syms[g_sym_i]);

   // 每个新组合重置分片游标与探测标志
   g_bars_total = 0;
   g_empty      = 0;
   g_probe      = ProbeFirst;
   g_cursor     = TimeCurrent();
}

//+------------------------------------------------------------------+
//| 全部完成：默认空转驻留；AutoDetach 则借图表重载卸载自己          |
//| （MQL5 无"移除 EA"API；TemplateSave 会弹保存对话框，故不采用）   |
//+------------------------------------------------------------------+
void FinishAll()
{
   g_done = true;
   EventKillTimer();

   // 终端图表最大 K 线数上限（超出部分不下载），供参考
   int maxbars = (int)TerminalInfoInteger(TERMINAL_MAXBARS);
   PrintFormat("=== ALL DONE %d 个品种（终端最大 K 线数上限 %d，"
               "Python 端 copy_rates_range/from_pos 可直接读取）", g_sym_n, maxbars);

   if(!AutoDetach)
   {
      Comment("AutoHistoryDownload 完成，EA 空转驻留（需再次下载直接重挂即可）");
      return;
   }

   // 防反复卸载死循环：同一终端会话只允许借重载卸载一次（终端全局变量随终端
   // 重启清空）；第二次跑完（模板重载场景）仅空转提示，不再触发重载
   string cnt_key = "AHD_detach_cnt";
   double cnt     = (GlobalVariableCheck(cnt_key) != 0) ? GlobalVariableGet(cnt_key) : 0.0;
   if(cnt >= 1.0)
   {
      Comment("AutoHistoryDownload 完成（模板内 EA 需手工移除）");
      return;
   }

   // chart_id 传 0 = 本 EA 所挂图表（MQL5 无 CHART_ID 图表属性常量可取自身 ID）；
   // 周期用 Period() 取当前图表周期（不改用户视角）：MQL5 也无 CHART_TIMEFRAME。
   // 普通挂上去的 EA 在图表重载后不再被附着 → 等效卸载；
   // 模板里的 EA 会重挂，但逐组合探测秒过 + 计数器防循环
   bool ok = ChartSetSymbolPeriod(0, Symbol(), Period());
   GlobalVariableSet(cnt_key, cnt + 1.0);
   PrintFormat("AutoDetach：%s（图表重载实现；模板内 EA 将重挂并秒过空转）",
               ok ? "已触发卸载请求" : "未受理，请手工移除");
   Comment("AutoHistoryDownload 完成，详见日志 (Ctrl+Shift+G)");
}

//+------------------------------------------------------------------+
//| EA 初始化：枚举建队 + 装定时器，下载在 OnTimer 分片执行          |
//+------------------------------------------------------------------+
int OnInit()
{
   g_started  = false;
   g_done     = false;
   g_sym_i = 0; g_tf_i = 0;
   g_last_sym = -1; g_last_tf = -1;

   g_pending_dl = InitDownload();        // false = 无匹配品种或数据已够深，空转
   if(!g_pending_dl)
   {
      g_done = true;
      return INIT_SUCCEEDED;
   }

   EventSetTimer(1);                     // 1 秒一次 tick，每次只推一片
   Comment("AutoHistoryDownload EA 运行中：正在下载历史数据...");
   return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| 反初始化：停定时器                                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
   Comment("");
}

//+------------------------------------------------------------------+
//| Timer：每次 tick 处理一个分片；下载逻辑与原脚本完全一致          |
//+------------------------------------------------------------------+
void OnTimer()
{
   if(g_done) return;
   if(!g_started)
   {
      if(!g_pending_dl) return;           // InitDownload 失败后不再重试
      g_started = true;
   }

   //--- 探测阶段：本品种×周期缓存已触底 → 直接移到下一组合，不重复下载
   if(g_probe)
   {
      g_probe = false;
      if(ProbeCurrentPair())
      {
         AdvanceState();
         return;
      }
   }

   bool tf_finished = ProcessOneSlice();
   if(tf_finished)
      AdvanceState();
   else
   {
      // 游标已触底（理论上 ProcessOneSlice 已拦下，双保险）
      if(g_cursor <= g_floor)
         AdvanceState();
   }

   if(!g_done && (g_last_sym != g_sym_i || g_last_tf != g_tf_i))
   {
      g_last_sym = g_sym_i; g_last_tf = g_tf_i;   // 仅在进度推进时重画 Comment，防每秒刷图
      Comment(StringFormat("AutoHistoryDownload EA 运行中：品种 %d/%d [%s] × 周期 %d/%d [%s]，已同步 %I64d 根",
                           g_sym_i + 1, g_sym_n,
                           (g_sym_i < g_sym_n ? g_syms[g_sym_i] : "-"),
                           g_tf_i + 1, g_tf_n,
                           (g_tf_i < g_tf_n ? g_tfs[g_tf_i] : "-"),
                           g_bars_total));
   }
}

//--- EA 不使用 tick 驱动（行情波动不影响下载进度）
void OnTick() {}
