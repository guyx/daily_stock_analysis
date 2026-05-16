# Quant Hub 数据基座集成（A 股增强，opt-in）

> 适用场景：你同时维护本地的 [`Quantitative_Data_Hub`](#) 数据基座项目，希望把基座产出的高质量 A 股截面数据（基本面派生指标、资金流分单、公司行为、bad_stocks 风险）作为额外上下文喂给 LLM，提升 A 股分析报告的深度。

## 目录

- [适用人群](#适用人群)
- [启用方式](#启用方式)
- [数据要求](#数据要求)
- [注入到 prompt 的内容](#注入到-prompt-的内容)
- [陈旧数据降级](#陈旧数据降级)
- [边界与已知限制](#边界与已知限制)
- [与现有 data_provider 的关系](#与现有-data_provider-的关系)
- [常见问题](#常见问题)

## 适用人群

仅适合**自己同时跑 Quantitative_Data_Hub 落盘流程**的用户。如果你只是公开部署 daily_stock_analysis、用 GitHub Actions 或 Docker 给别人用，**不需要也无法启用**这套增强——别人那边没有 `E:/Quant_Data` 目录，配置了 `QUANT_HUB_DATA_ROOT` 也只会 `is_available=False` 然后自动跳过。

## 启用方式

在 `.env` 中加入两行：

```bash
# 必填：本地 Quantitative_Data_Hub 数据基座的根目录
QUANT_HUB_DATA_ROOT=E:/Quant_Data

# 可选：数据陈旧时回溯交易日上限（默认 3 个交易日）
QUANT_HUB_MAX_STALE_DAYS=3
```

未配置 `QUANT_HUB_DATA_ROOT` 或目录不存在 ⇒ 整套机制不激活，pipeline 行为完全等同于改动前。

启动日志确认：

```
INFO  Quant Hub context service enabled (A-shares, data_root=E:/Quant_Data)
```

## 数据要求

service 在每次 A 股分析时按需读取以下 parquet。**缺哪个数据集就跳过哪个子段**，整体不会报错：

| 数据集 | 路径模板 | 用途 |
|---|---|---|
| `assessment_daily` | `{root}/assessment_daily/{YYYYMMDD}.parquet` | 基本面派生指标（30 + 30 _annual） |
| `merged/daily` | `{root}/merged/daily/{YYYYMMDD}.parquet` | 1 日 / 5 日复权涨幅 |
| `capital_flow/moneyflow` | `{root}/capital_flow/moneyflow/{YYYYMMDD}.parquet` | 近 30 日资金流聚合 |
| `corporate_actions/by_stock` | `{root}/corporate_actions/by_stock/{code}.parquet` | 近 90 日回购 / 增减持事件 |
| `bad_stocks` | `{root}/bad_stocks/{YYYYMMDD}.parquet` | 风险黑名单命中检测 |
| `external/trade_calendar` | `{root}/external/trade_calendar.parquet` | 交易日历，用于陈旧回溯与近 30 日聚合 |

注意 Hub 内部 `code` 列是带后缀形态（`600519.SH`、`000001.SZ`、`920019.BJ`），service 内部自动从 daily_stock_analysis 的 6 位入参做归一，规则与 Hub `src/utils/stock_utils.py:normalize_stock_code` 保持一致：

- `6XXXXX` → `.SH`
- `00XXXX` / `30XXXX` → `.SZ`
- `920XXX` → `.BJ`
- 其他形态返回 None，对应代码视为非 A 股、不注入

## 注入到 prompt 的内容

启用后会在 LLM prompt 的 `## 📰 舆情情报` 段之后追加一段独立的 `## 📊 量化数据补充`，结构如下（示例）：

```markdown
## 📊 量化数据补充

> 来自本地 Quantitative_Data_Hub 数据基座（A 股，T-1 起算），与上方实时行情互补，请用于交叉验证基本面、资金面与公司行为信号。

### 基本面（截至 2026-05-15）
- 元数据/估值: 行业 白酒, PE-TTM 22.30, PB 7.80, 股息率 1.80%
- 盈利: ROE 23.1%（上年年报 21.5%）, 毛利 75.2%, 营业利润率 38.4%
- 利润质量: 扣非占比 96.2%, 投资损益占比 1.3%
- 偿债: 资产负债率 21.3%, 速动比率 2.41, 商誉占比 0.0%
- 现金流: 经营现金/营业利润 1.12, FCFF/总资产 18.0%
- 成长(TTM 同比): 营收 +12.3%, 净利 +15.8%, 经营现金流 +8.1%

### 行情
- 涨幅（截至 2026-05-15）: 近 1 日 -1.23%, 近 5 日 +2.10%
- 资金流（近 30 个交易日命中）: 主力近 5 日 +2.10 亿 / 近 30 日 +8.20 亿; 散户近 30 日 -3.40 亿 （主力净流入，散户净流出，分歧明显）

### 公司行为（近 90 日）
- 计数: 回购 1 起, 增持 2 起
- 2026-04-22 回购（实施中）：以集中竞价方式回购公司股份
- 2026-04-15 增持 控股股东 +0.50%（实施完成）
- 2026-03-08 增持 高管 +0.05%（实施完成）

### 风险（截至 2026-05-15）
- 未在 bad_stocks 黑名单
```

字段口径与精度：

| 字段 | Hub 原始 | 输出 |
|---|---|---|
| `metric_*`（百分比类） | 小数（如 0.231） | `23.1%` |
| `dv_ttm`（股息率 TTM） | **百分数**（如 3.87，来自 Tushare daily_basic） | `3.87%`（不二次乘 100） |
| `pct_chg` / `pct_5chg`（merged/daily） | 小数（如 -0.0113） | `±XX.XX%` |
| `capital_flow.amount`（各档买卖额） | **万元**（Tushare moneyflow 标准口径，Hub DATA_INFO.md 写"元"不准） | `±XX.XX 亿` |
| `pe_ttm` / `pb` / 比率类 | 数值 | 保留 2 位小数 |
| 日期 | `YYYYMMDD` | `YYYY-MM-DD` |

## 陈旧数据降级

每个数据集独立判定截至日：

1. 优先尝试 `as_of`（默认今天，可选传入）；
2. 当日缺失则沿 Hub 的交易日历回溯，最多回溯 `QUANT_HUB_MAX_STALE_DAYS` 个交易日（默认 3），找到的最近一日生效；段落小标题会带 `，陈旧 N 个交易日` 标注；
3. 仍未找到 ⇒ 对应子段直接跳过（不输出占位）；
4. 整套 service 异常或 `data_root` 不存在 ⇒ 跳过整段，pipeline 退化到现状。

如果某个子段（比如公司行为按票 parquet）天然就没有数据，会输出一行明确的"暂无回购 / 增减持记录"，避免 LLM 误以为是程序故障。

## 边界与已知限制

- **仅 A 股**：港股、美股不注入。判断逻辑用 `src/market_context.py:detect_market`，与 prompt 其余部分一致。
- **不动 Agent 模式**：`src/agent/` 的多轮追问路径走的是另一套上下文装配，**第一期不接入**。如果你日常用 Agent 模式问股，看不到本段。
- **不动历史报告 / 回测**：service 实例为 pipeline 单例，按需读取，**不参与历史快照重放**。
- **北交所（.BJ）**：Hub 数据覆盖可能不全；找不到 `code` 时 service 静默跳过对应子段。
- **公司行为字段口径**：Tushare 数值字段（vol/amount/change_vol）不准，service 优先用 Akshare 文本（`source='a'`、`proc`、`ak_title`）。如需精确数值请回到 Hub 原始文件确认。

## 与现有 data_provider 的关系

**互补而非替代**。`data_provider/` 仍然是默认行情来源（实时请求 + 多源 fallback，覆盖 A/H/US），Quant Hub 只是在 A 股分析时**额外**塞一段质量更高、维度更深的补充上下文。

- 不修改 `data_provider/` 任何 fetcher。
- 不接管 prompt 现有的"今日行情"/"主力资金流向"/"财报与分红"段——LLM 同时看到两份，自己消化（重复字段口径相近，无冲突）。
- 不参与决策合成（如 `stabilize_decision_with_structure`）——只是给 LLM 多一段输入信息。

## 常见问题

**Q：我配了 `QUANT_HUB_DATA_ROOT` 但分析报告里没看到"量化数据补充"段？**

按以下顺序排查：

1. 日志里有没有 `Quant Hub context service enabled (A-shares, data_root=...)`？没有的话说明配置没读到（注意 `.env` 是否生效）。
2. 分析的是不是 A 股？港股、美股不会注入。
3. `{data_root}/assessment_daily/{今日}.parquet` 是否存在？如果今天的、近 3 个交易日的都没有，整个基本面段会跳过；其他子段同理。
4. 看日志有没有 `Quant Hub context injected (N chars)`，没有的话说明所有子段都没有内容产出。

**Q：我跑 Docker / GitHub Actions 配 `QUANT_HUB_DATA_ROOT` 有用吗？**

没用。Docker 容器里没有 `E:/Quant_Data` 卷映射，`is_available` 自动为 False。如果想用，需要把 Hub 的 parquet 目录挂载进容器，目前不推荐（数据量大、更新链路复杂）。

**Q：会不会拖慢分析速度？**

不会。service 内部有 600 秒 TTL 缓存（按 dataset+date 做 key），批量分析 N 只股票时同一天的 `assessment_daily` 只读 1 次。单股注入开销 < 50 ms。

**Q：怎么彻底关掉？**

把 `.env` 里的 `QUANT_HUB_DATA_ROOT` 注释掉或设为空。也可以临时 `unset QUANT_HUB_DATA_ROOT` 启动。
