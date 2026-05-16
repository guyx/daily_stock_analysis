# -*- coding: utf-8 -*-
"""
===================================
Quant Hub Context Service
===================================

读取本地 Quantitative_Data_Hub 项目产出的 Parquet 数据集，
按股票代码组装一段 markdown 上下文，注入 LLM 分析 prompt。

可选模块——需要配置 ``QUANT_HUB_DATA_ROOT`` 环境变量。
仅对 A 股代码生效；港股 / 美股 / 非 A 股自动跳过。

依赖的数据集：
    {data_root}/assessment_daily/{YYYYMMDD}.parquet       基本面派生指标横截面
    {data_root}/merged/daily/{YYYYMMDD}.parquet           日频行情与估值
    {data_root}/capital_flow/moneyflow/{YYYYMMDD}.parquet 资金流分单类型
    {data_root}/corporate_actions/by_stock/{code}.parquet 公司行为（按票）
    {data_root}/bad_stocks/{YYYYMMDD}.parquet             风险黑名单
    {data_root}/external/trade_calendar.parquet           交易日历

设计要点：
- ``is_available`` 仅在 data_root 配置且目录存在时为 True。
- 每个数据集独立判定"截至日"：先尝试 ``as_of_date``，缺失则
  沿交易日历回溯最多 ``max_stale_days``（默认 3 个交易日）。
- 任何 IO / 解析异常都吞到 logger.warning，最终返回 None
  让 pipeline 安全降级。
- 进程内 ``threading.RLock`` 保护的 TTL 缓存 (600 秒)
  防止批量分析时同一份按日 parquet 被多次读盘。
"""

from __future__ import annotations

import bisect
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def _to_hub_code(code: str) -> Optional[str]:
    """6 位 A 股代码 -> Hub 形态 (XXXXXX.SH/SZ/BJ)。

    规则与 ``Quantitative_Data_Hub/src/utils/stock_utils.py:normalize_stock_code`` 保持一致：
    6 开头 -> .SH；00/30 开头 -> .SZ；920 开头 -> .BJ；其余视为非 A 股返回 None。
    """
    if not code or not isinstance(code, str):
        return None
    code = code.strip()
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith("6"):
        return f"{code}.SH"
    if code.startswith(("00", "30")):
        return f"{code}.SZ"
    if code.startswith("920"):
        return f"{code}.BJ"
    return None


class QuantHubContextService:
    """A 股 LLM 上下文增强 service，读 Hub 落盘 parquet 拼 markdown 段落。"""

    _CACHE_TTL = 600  # seconds
    _CALENDAR_CACHE_TTL = 3600  # 交易日历变动慢，缓存久一点

    _FORECAST_TYPE_LABELS: Dict[int, str] = {
        20: "预亏",
        40: "预减",
        55: "续亏",
        65: "略减",
        70: "扭亏",
        80: "略增",
        90: "续盈",
        100: "预增",
    }

    def __init__(self, data_root: Optional[str] = None, max_stale_days: int = 3):
        root = (data_root or "").strip() or None
        self._root: Optional[Path] = Path(root) if root else None
        self._max_stale_days = max(0, int(max_stale_days))
        self._cache: Dict[str, Tuple[float, Any]] = {}
        self._cache_lock = threading.RLock()
        self._calendar: Optional[List[str]] = None
        self._calendar_loaded_at: float = 0.0

    # ------------------------------------------------------------------
    # Public surface
    # ------------------------------------------------------------------

    @property
    def is_available(self) -> bool:
        return self._root is not None and self._root.exists()

    def get_quant_hub_context(self, code: str, as_of_date: Optional[str] = None) -> Optional[str]:
        """返回当只 A 股的量化补充 markdown 段落；不可用时返回 None。

        Args:
            code: 6 位 A 股代码（与 daily_stock_analysis pipeline 入参一致，不带后缀）。
            as_of_date: 可选 YYYYMMDD；默认取今日。
        """
        if not self.is_available:
            return None
        hub_code = _to_hub_code(code)
        if hub_code is None:
            return None

        try:
            as_of = self._normalize_as_of(as_of_date)
            sections: List[str] = []
            for builder in (
                self._build_fundamental_section,
                self._build_quote_section,
                self._build_corp_actions_section,
                self._build_risk_section,
            ):
                try:
                    text = builder(hub_code, as_of)
                except Exception as exc:
                    logger.warning(
                        "[quant_hub] section %s failed for %s: %s",
                        builder.__name__, code, exc, exc_info=True,
                    )
                    text = None
                if text:
                    sections.append(text)
            if not sections:
                return None
            return "\n\n".join(sections)
        except Exception as exc:
            logger.warning("[quant_hub] context build failed for %s: %s", code, exc, exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Sections
    # ------------------------------------------------------------------

    def _build_fundamental_section(self, hub_code: str, as_of: str) -> Optional[str]:
        date_used, stale = self._resolve_dataset_date(("assessment_daily",), as_of)
        if date_used is None:
            return None
        path = self._root / "assessment_daily" / f"{date_used}.parquet"  # type: ignore[operator]
        df = self._read_parquet_cached(path, key=f"assessment_daily:{date_used}")
        if df is None or df.empty or "code" not in df.columns:
            return None
        hit = df[df["code"] == hub_code]
        if hit.empty:
            return f"### 基本面（截至 {self._fmt_date(date_used)}{self._stale_suffix(stale)}）\n- Hub 暂无 {hub_code} 的基本面数据"
        r = hit.iloc[0]

        lines = [f"### 基本面（截至 {self._fmt_date(date_used)}{self._stale_suffix(stale)}）"]

        # 元数据 / 估值
        meta_bits: List[str] = []
        industry = self._safe_str(r.get("industry"))
        if industry:
            meta_bits.append(f"行业 {industry}")
        pe_ttm = self._safe_num(r.get("pe_ttm"))
        if pe_ttm is not None:
            meta_bits.append(f"PE-TTM {pe_ttm:.2f}")
        pb = self._safe_num(r.get("pb"))
        if pb is not None:
            meta_bits.append(f"PB {pb:.2f}")
        dv_ttm = self._safe_num(r.get("dv_ttm"))
        if dv_ttm is not None:
            # Hub 的 dv_ttm 来自 Tushare daily_basic，单位已经是百分比（3.87 即 3.87%），无需乘 100
            meta_bits.append(f"股息率 {dv_ttm:.2f}%")
        if meta_bits:
            lines.append(f"- 元数据/估值: {', '.join(meta_bits)}")

        # 盈利能力（roe 带上年年报对照）
        profit_bits = list(filter(None, [
            self._fmt_pct_with_annual(r, "roe", "ROE"),
            self._fmt_pct(r, "gross_margin", "毛利"),
            self._fmt_pct(r, "operate_margin", "营业利润率"),
        ]))
        if profit_bits:
            lines.append(f"- 盈利: {', '.join(profit_bits)}")

        # 利润质量
        quality_bits = list(filter(None, [
            self._fmt_pct(r, "dt_profit_ratio", "扣非占比"),
            self._fmt_pct(r, "invest_profit_ratio", "投资损益占比"),
        ]))
        if quality_bits:
            lines.append(f"- 利润质量: {', '.join(quality_bits)}")

        # 偿债
        debt_bits = list(filter(None, [
            self._fmt_pct(r, "debt_to_assets", "资产负债率"),
            self._fmt_ratio(r, "quick_ratio", "速动比率"),
            self._fmt_pct(r, "goodwill_ratio", "商誉占比"),
        ]))
        if debt_bits:
            lines.append(f"- 偿债: {', '.join(debt_bits)}")

        # 现金流
        cash_bits = list(filter(None, [
            self._fmt_ratio(r, "cfo_earnings_ratio", "经营现金/营业利润"),
            self._fmt_pct(r, "fcff_ratio", "FCFF/总资产"),
        ]))
        if cash_bits:
            lines.append(f"- 现金流: {', '.join(cash_bits)}")

        # TTM 同比（增长率，带正负号便于 LLM 识别方向）
        growth_bits = list(filter(None, [
            self._fmt_pct(r, "revenue_ttm_yoy", "营收", signed=True),
            self._fmt_pct(r, "income_ttm_yoy", "净利", signed=True),
            self._fmt_pct(r, "ocf_ttm_yoy", "经营现金流", signed=True),
        ]))
        if growth_bits:
            lines.append(f"- 成长(TTM 同比): {', '.join(growth_bits)}")

        # 业绩预告
        d2_score = self._safe_num(r.get("metric_d2_type_score"))
        if d2_score is not None:
            label = self._FORECAST_TYPE_LABELS.get(int(round(d2_score)))
            if label:
                lines.append(f"- 业绩预告: {label}（档位分 {int(round(d2_score))}）")

        if len(lines) <= 1:
            return None
        return "\n".join(lines)

    def _build_quote_section(self, hub_code: str, as_of: str) -> Optional[str]:
        merged_date, merged_stale = self._resolve_dataset_date(("merged", "daily"), as_of)
        cf_summary = self._aggregate_capital_flow(hub_code, as_of)

        if merged_date is None and cf_summary is None:
            return None

        lines = ["### 行情"]

        if merged_date is not None:
            merged_path = self._root / "merged" / "daily" / f"{merged_date}.parquet"  # type: ignore[operator]
            df = self._read_parquet_cached(
                merged_path,
                key=f"merged/daily:{merged_date}",
                columns=["code", "pct_chg", "pct_5chg"],
            )
            quote_bits: List[str] = []
            row = None
            if df is not None and not df.empty and "code" in df.columns:
                hit = df[df["code"] == hub_code]
                if not hit.empty:
                    row = hit.iloc[0]
            if row is not None:
                pct1 = self._safe_num(row.get("pct_chg"))
                pct5 = self._safe_num(row.get("pct_5chg"))
                if pct1 is not None:
                    quote_bits.append(f"近 1 日 {pct1 * 100:+.2f}%")
                if pct5 is not None:
                    quote_bits.append(f"近 5 日 {pct5 * 100:+.2f}%")
            head = f"截至 {self._fmt_date(merged_date)}{self._stale_suffix(merged_stale)}"
            if quote_bits:
                lines.append(f"- 涨幅（{head}）: {', '.join(quote_bits)}")
            elif row is None:
                lines.append(f"- 涨幅（{head}）: Hub 暂无 {hub_code} 行情记录")

        if cf_summary:
            lines.append(cf_summary)

        if len(lines) <= 1:
            return None
        return "\n".join(lines)

    def _aggregate_capital_flow(self, hub_code: str, as_of: str) -> Optional[str]:
        calendar = self._load_trade_calendar()
        if not calendar:
            return None
        idx = self._find_last_calendar_day(calendar, as_of)
        if idx < 0:
            return None
        window_30 = calendar[max(0, idx - 29): idx + 1]
        window_5_set = set(calendar[max(0, idx - 4): idx + 1])

        net_main_30 = 0.0
        net_main_5 = 0.0
        net_retail_30 = 0.0
        hits = 0

        for d in window_30:
            path = self._root / "capital_flow" / "moneyflow" / f"{d}.parquet"  # type: ignore[operator]
            df = self._read_parquet_cached(
                path,
                key=f"capital_flow/moneyflow:{d}",
                columns=[
                    "code",
                    "buy_lg_amount", "sell_lg_amount",
                    "buy_elg_amount", "sell_elg_amount",
                    "buy_sm_amount", "sell_sm_amount",
                ],
            )
            if df is None or df.empty or "code" not in df.columns:
                continue
            row = df[df["code"] == hub_code]
            if row.empty:
                continue
            r = row.iloc[0]
            main_net = (
                self._safe_num(r.get("buy_lg_amount"), 0.0)
                + self._safe_num(r.get("buy_elg_amount"), 0.0)
                - self._safe_num(r.get("sell_lg_amount"), 0.0)
                - self._safe_num(r.get("sell_elg_amount"), 0.0)
            )
            retail_net = (
                self._safe_num(r.get("buy_sm_amount"), 0.0)
                - self._safe_num(r.get("sell_sm_amount"), 0.0)
            )
            net_main_30 += main_net
            net_retail_30 += retail_net
            if d in window_5_set:
                net_main_5 += main_net
            hits += 1

        if hits == 0:
            return None

        def fmt_yi(amount_wanyuan: float) -> str:
            # Hub 的 capital_flow.amount 沿用 Tushare moneyflow 口径，单位是万元，不是元
            return f"{amount_wanyuan / 1e4:+.2f} 亿"

        divergence = ""
        if net_main_30 > 0 and net_retail_30 < 0:
            divergence = "（主力净流入，散户净流出，分歧明显）"
        elif net_main_30 < 0 and net_retail_30 > 0:
            divergence = "（主力净流出，散户净流入，分歧明显）"
        elif net_main_30 > 0 and net_retail_30 > 0:
            divergence = "（主力 / 散户同向净流入）"
        elif net_main_30 < 0 and net_retail_30 < 0:
            divergence = "（主力 / 散户同向净流出）"

        return (
            f"- 资金流（近 {hits} 个交易日命中）: "
            f"主力近 5 日 {fmt_yi(net_main_5)} / 近 30 日 {fmt_yi(net_main_30)}; "
            f"散户近 30 日 {fmt_yi(net_retail_30)}{(' ' + divergence) if divergence else ''}"
        )

    def _build_corp_actions_section(self, hub_code: str, as_of: str) -> Optional[str]:
        path = self._root / "corporate_actions" / "by_stock" / f"{hub_code}.parquet"  # type: ignore[operator]
        df = self._read_parquet_cached(path, key=f"corporate_actions/by_stock:{hub_code}")
        header = "### 公司行为（近 90 日）"
        if df is None or df.empty or "publish_date" not in df.columns:
            return f"{header}\n- 暂无回购 / 增减持记录"
        try:
            as_of_dt = datetime.strptime(as_of, "%Y%m%d")
            cutoff = (as_of_dt - timedelta(days=90)).strftime("%Y%m%d")
            pub = df["publish_date"].astype(str)
            mask = (pub >= cutoff) & (pub <= as_of)
            recent = df[mask].copy()
            recent["publish_date"] = pub[mask]
        except Exception as exc:
            logger.warning("[quant_hub] corp_actions date filter failed for %s: %s", hub_code, exc)
            return f"{header}\n- 日期过滤失败，跳过"

        if recent.empty:
            return f"{header}\n- 近 90 日无回购 / 增减持事件"

        lines = [header]
        counts: List[str] = []
        if "action_type" in recent.columns:
            repurchase = recent[recent["action_type"] == "repurchase"]
            holder = recent[recent["action_type"] == "holdertrade"]
            if not repurchase.empty:
                counts.append(f"回购 {len(repurchase)} 起")
            if not holder.empty and "in_de" in holder.columns:
                inc = holder[holder["in_de"] == "IN"]
                dec = holder[holder["in_de"] == "DE"]
                if not inc.empty:
                    counts.append(f"增持 {len(inc)} 起")
                if not dec.empty:
                    counts.append(f"减持 {len(dec)} 起")
        if counts:
            lines.append(f"- 计数: {', '.join(counts)}")

        recent_sorted = recent.sort_values("publish_date", ascending=False).head(5)
        for _, ev in recent_sorted.iterrows():
            text = self._format_corp_action_event(ev)
            if text:
                lines.append(f"  - {text}")

        return "\n".join(lines)

    def _format_corp_action_event(self, ev: pd.Series) -> Optional[str]:
        date_raw = self._safe_str(ev.get("publish_date"))
        if not date_raw:
            return None
        date_fmt = self._fmt_date(date_raw)
        action = self._safe_str(ev.get("action_type"))
        proc = self._safe_str(ev.get("proc"))
        if action == "repurchase":
            ak_title = self._safe_str(ev.get("ak_title"))
            text = f"{date_fmt} 回购"
            if proc:
                text += f"（{proc}）"
            if ak_title:
                text += f"：{ak_title[:60]}"
            return text
        if action == "holdertrade":
            in_de = self._safe_str(ev.get("in_de"))
            kind = "增持" if in_de == "IN" else ("减持" if in_de == "DE" else "持股变动")
            holder = self._safe_str(ev.get("holder_name"))
            text = f"{date_fmt} {kind}"
            if holder:
                text += f" {holder[:30]}"
            ratio = self._safe_num(ev.get("change_ratio"))
            if ratio is not None:
                text += f" {ratio:+.2f}%"
            if proc:
                text += f"（{proc}）"
            return text
        return f"{date_fmt} {action or '事件'}"

    def _build_risk_section(self, hub_code: str, as_of: str) -> Optional[str]:
        date_used, stale = self._resolve_dataset_date(("bad_stocks",), as_of)
        if date_used is None:
            return None
        path = self._root / "bad_stocks" / f"{date_used}.parquet"  # type: ignore[operator]
        df = self._read_parquet_cached(
            path,
            key=f"bad_stocks:{date_used}",
            columns=["code", "reason"],
        )
        if df is None or df.empty or "code" not in df.columns:
            return None
        header = f"### 风险（截至 {self._fmt_date(date_used)}{self._stale_suffix(stale)}）"
        hit = df[df["code"] == hub_code]
        if hit.empty:
            return f"{header}\n- 未在 bad_stocks 黑名单"
        reason = self._safe_str(hit.iloc[0].get("reason")) or "（原因未知）"
        return f"{header}\n- ⚠️ 命中 bad_stocks 黑名单：{reason}"

    # ------------------------------------------------------------------
    # IO / cache helpers
    # ------------------------------------------------------------------

    def _read_parquet_cached(
        self,
        path: Path,
        *,
        key: str,
        columns: Optional[List[str]] = None,
    ) -> Optional[pd.DataFrame]:
        now = time.monotonic()
        with self._cache_lock:
            cached = self._cache.get(key)
            if cached and (now - cached[0]) < self._CACHE_TTL:
                return cached[1]

        if not path.exists():
            with self._cache_lock:
                self._cache[key] = (now, None)
            return None
        try:
            if columns:
                df = pd.read_parquet(path, columns=columns)
            else:
                df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("[quant_hub] failed to read %s: %s", path, exc)
            with self._cache_lock:
                self._cache[key] = (now, None)
            return None

        with self._cache_lock:
            self._cache[key] = (time.monotonic(), df)
        return df

    def _resolve_dataset_date(self, dataset_parts: Tuple[str, ...], as_of: str) -> Tuple[Optional[str], int]:
        """走交易日历回溯找最近可用文件；找不到返回 (None, 0)。

        dataset_parts 是相对 data_root 的目录拆分，例如 ("merged", "daily")。
        """
        if self._root is None:
            return None, 0
        rel_dir = self._root
        for seg in dataset_parts:
            rel_dir = rel_dir / seg

        calendar = self._load_trade_calendar()
        candidates: List[str] = []
        if calendar:
            idx = self._find_last_calendar_day(calendar, as_of)
            if idx >= 0:
                start = max(0, idx - self._max_stale_days)
                candidates = list(reversed(calendar[start: idx + 1]))
        if not candidates:
            try:
                base = datetime.strptime(as_of, "%Y%m%d")
                candidates = [
                    (base - timedelta(days=i)).strftime("%Y%m%d")
                    for i in range(self._max_stale_days + 1)
                ]
            except Exception:
                candidates = [as_of]

        for stale_days, d in enumerate(candidates):
            if (rel_dir / f"{d}.parquet").exists():
                return d, stale_days
        return None, 0

    def _load_trade_calendar(self) -> Optional[List[str]]:
        if self._root is None:
            return None
        now = time.monotonic()
        if self._calendar is not None and (now - self._calendar_loaded_at) < self._CALENDAR_CACHE_TTL:
            return self._calendar
        path = self._root / "external" / "trade_calendar.parquet"
        if not path.exists():
            return None
        try:
            cal = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("[quant_hub] failed to read trade_calendar: %s", exc)
            return None
        if "is_open" not in cal.columns or "date" not in cal.columns:
            return None
        try:
            days = cal.loc[cal["is_open"].astype(int) == 1, "date"].astype(str).tolist()
        except Exception as exc:
            logger.warning("[quant_hub] failed to parse trade_calendar: %s", exc)
            return None
        days.sort()
        self._calendar = days
        self._calendar_loaded_at = now
        return days

    @staticmethod
    def _find_last_calendar_day(calendar: List[str], as_of: str) -> int:
        if not calendar:
            return -1
        idx = bisect.bisect_right(calendar, as_of) - 1
        return idx if idx >= 0 else -1

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_as_of(as_of_date: Optional[str]) -> str:
        candidate = (as_of_date or "").strip()
        if len(candidate) == 8 and candidate.isdigit():
            return candidate
        return datetime.now().strftime("%Y%m%d")

    @staticmethod
    def _fmt_date(d: str) -> str:
        if d and len(d) == 8 and d.isdigit():
            return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
        return d or ""

    @staticmethod
    def _stale_suffix(stale_days: int) -> str:
        return f"，陈旧 {stale_days} 个交易日" if stale_days > 0 else ""

    @staticmethod
    def _safe_str(v: Any) -> str:
        if v is None:
            return ""
        try:
            if pd.isna(v):
                return ""
        except (TypeError, ValueError):
            pass
        return str(v).strip()

    @staticmethod
    def _safe_num(v: Any, default: Optional[float] = None) -> Optional[float]:
        if v is None:
            return default
        try:
            if pd.isna(v):
                return default
        except (TypeError, ValueError):
            pass
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    @classmethod
    def _fmt_pct(cls, r: pd.Series, field: str, label: str, *, signed: bool = False) -> Optional[str]:
        v = cls._safe_num(r.get(f"metric_{field}"))
        if v is None:
            return None
        fmt = "+.1f" if signed else ".1f"
        return f"{label} {v * 100:{fmt}}%"

    @classmethod
    def _fmt_ratio(cls, r: pd.Series, field: str, label: str, decimals: int = 2) -> Optional[str]:
        v = cls._safe_num(r.get(f"metric_{field}"))
        if v is None:
            return None
        return f"{label} {v:.{decimals}f}"

    @classmethod
    def _fmt_pct_with_annual(cls, r: pd.Series, field: str, label: str) -> Optional[str]:
        cur = cls._safe_num(r.get(f"metric_{field}"))
        if cur is None:
            return None
        text = f"{label} {cur * 100:.1f}%"
        prev = cls._safe_num(r.get(f"metric_{field}_annual"))
        # 并列展示当期与上年年报值（不计算 pp 差值——当期可能是季度累计，与年度不可直接相减）
        if prev is not None:
            text += f"（上年年报 {prev * 100:.1f}%）"
        return text
