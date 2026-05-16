# -*- coding: utf-8 -*-
"""Tests for QuantHubContextService.

覆盖：
- is_available 各种边界
- 6 位代码 → Hub 后缀格式归一化
- 完整命中（基本面 / 行情 / 公司行为 / 风险）
- 陈旧数据沿交易日历回溯（A）
- 超出 max_stale_days 跳过子段（B）
- 非 A 股代码（港股 / 美股）返回 None
- bad_stocks 命中与未命中
- TTL 缓存命中（同一 dataset+date 不重复读盘）
- data_root 不存在
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Iterable, List
from unittest.mock import patch

import pandas as pd

from src.services.quant_hub_context_service import QuantHubContextService, _to_hub_code


# ----------------------------------------------------------------------
# Fixture builders
# ----------------------------------------------------------------------


def _trade_calendar(days: Iterable[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "date": list(days),
        "is_open": [1] * len(list(days)),
        "source": ["test"] * len(list(days)),
    })


def _assessment_row(code: str, **kwargs) -> dict:
    base = {
        "code": code,
        "name": "测试股",
        "industry": "白酒",
        "total_mv": 1.2e12,
        "circ_mv": 1.2e12,
        "pe_ttm": 22.3,
        "pb": 7.8,
        "dv_ttm": 1.8,    # 百分比形式（Tushare daily_basic 原始单位，不是小数）
        "metric_roe": 0.231,
        "metric_roe_annual": 0.215,
        "metric_gross_margin": 0.752,
        "metric_gross_margin_annual": 0.749,
        "metric_operate_margin": 0.384,
        "metric_dt_profit_ratio": 0.962,
        "metric_invest_profit_ratio": 0.013,
        "metric_debt_to_assets": 0.213,
        "metric_quick_ratio": 2.41,
        "metric_goodwill_ratio": 0.0,
        "metric_cfo_earnings_ratio": 1.12,
        "metric_fcff_ratio": 0.18,
        "metric_revenue_ttm_yoy": 0.123,
        "metric_income_ttm_yoy": 0.158,
        "metric_ocf_ttm_yoy": 0.081,
    }
    base.update(kwargs)
    return base


def _moneyflow_row(code: str, *, lg_net: float = 0.0, elg_net: float = 0.0, sm_net: float = 0.0) -> dict:
    """构造一行 moneyflow fixture。

    注意：lg_net / elg_net / sm_net 单位为「万元」（Tushare moneyflow 标准口径），
    与生产 Hub 数据保持一致；service 内部会按 1e4 转亿。
    """
    return {
        "code": code,
        "buy_lg_amount": max(lg_net, 0),
        "sell_lg_amount": max(-lg_net, 0),
        "buy_elg_amount": max(elg_net, 0),
        "sell_elg_amount": max(-elg_net, 0),
        "buy_sm_amount": max(sm_net, 0),
        "sell_sm_amount": max(-sm_net, 0),
        "trade_date": "20260515",
    }


def _build_root(
    tmp_root: Path,
    *,
    calendar_days: List[str],
    assessment: List[tuple],   # (date, [row dicts])
    merged: List[tuple] = None,
    moneyflow: List[tuple] = None,
    corp_actions: List[tuple] = None,  # (hub_code, [row dicts])
    bad_stocks: List[tuple] = None,
) -> Path:
    tmp_root.mkdir(parents=True, exist_ok=True)
    (tmp_root / "external").mkdir(exist_ok=True)
    _trade_calendar(calendar_days).to_parquet(tmp_root / "external" / "trade_calendar.parquet")

    asd = tmp_root / "assessment_daily"
    asd.mkdir(exist_ok=True)
    for date, rows in (assessment or []):
        pd.DataFrame(rows).to_parquet(asd / f"{date}.parquet")

    md = tmp_root / "merged" / "daily"
    md.mkdir(parents=True, exist_ok=True)
    for date, rows in (merged or []):
        pd.DataFrame(rows).to_parquet(md / f"{date}.parquet")

    mf = tmp_root / "capital_flow" / "moneyflow"
    mf.mkdir(parents=True, exist_ok=True)
    for date, rows in (moneyflow or []):
        pd.DataFrame(rows).to_parquet(mf / f"{date}.parquet")

    ca = tmp_root / "corporate_actions" / "by_stock"
    ca.mkdir(parents=True, exist_ok=True)
    for code, rows in (corp_actions or []):
        pd.DataFrame(rows).to_parquet(ca / f"{code}.parquet")

    bs = tmp_root / "bad_stocks"
    bs.mkdir(exist_ok=True)
    for date, rows in (bad_stocks or []):
        pd.DataFrame(rows).to_parquet(bs / f"{date}.parquet")

    return tmp_root


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------


class TestServiceAvailability(unittest.TestCase):
    def test_unavailable_without_root(self):
        svc = QuantHubContextService(data_root=None)
        self.assertFalse(svc.is_available)

    def test_unavailable_with_empty_root(self):
        svc = QuantHubContextService(data_root="   ")
        self.assertFalse(svc.is_available)

    def test_unavailable_when_root_missing(self):
        svc = QuantHubContextService(data_root="/nonexistent/quant_hub_path_xyz")
        self.assertFalse(svc.is_available)

    def test_available_when_root_exists(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            svc = QuantHubContextService(data_root=tmp)
            self.assertTrue(svc.is_available)


class TestCodeNormalization(unittest.TestCase):
    def test_shanghai_main_board(self):
        self.assertEqual(_to_hub_code("600519"), "600519.SH")
        self.assertEqual(_to_hub_code("688001"), "688001.SH")

    def test_shenzhen_main_and_chinext(self):
        self.assertEqual(_to_hub_code("000001"), "000001.SZ")
        self.assertEqual(_to_hub_code("300750"), "300750.SZ")

    def test_beijing(self):
        self.assertEqual(_to_hub_code("920019"), "920019.BJ")

    def test_invalid_inputs(self):
        self.assertIsNone(_to_hub_code(""))
        self.assertIsNone(_to_hub_code("hk00700"))   # 港股
        self.assertIsNone(_to_hub_code("AAPL"))      # 美股
        self.assertIsNone(_to_hub_code("12345"))     # 长度错
        self.assertIsNone(_to_hub_code("12345A"))    # 含字母
        self.assertIsNone(_to_hub_code("400123"))    # 不在 (00,30,6,920) 前缀范围
        self.assertIsNone(_to_hub_code(None))


class TestFullHit(unittest.TestCase):
    """完整命中：4 个段全部产出。"""

    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        as_of = "20260515"
        # 5 天交易日历，含 as_of
        calendar = ["20260511", "20260512", "20260513", "20260514", as_of]
        _build_root(
            self.root,
            calendar_days=calendar,
            assessment=[(as_of, [_assessment_row("600519.SH")])],
            merged=[(as_of, [{"code": "600519.SH", "pct_chg": -0.0123, "pct_5chg": 0.0210}])],
            moneyflow=[(d, [_moneyflow_row("600519.SH", lg_net=2e7, elg_net=3e7, sm_net=-1e7)]) for d in calendar],
            corp_actions=[("600519.SH", [
                {"publish_date": "20260422", "action_type": "repurchase",
                 "source": "a", "proc": "实施中", "ak_title": "以集中竞价方式回购公司股份"},
                {"publish_date": "20260415", "action_type": "holdertrade",
                 "in_de": "IN", "holder_name": "控股股东", "change_ratio": 0.5,
                 "source": "t", "proc": "实施完成"},
            ])],
            bad_stocks=[(as_of, [{"code": "OTHER.SH", "reason": "ST/退"}])],
        )
        self.svc = QuantHubContextService(data_root=str(self.root), max_stale_days=3)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_get_quant_hub_context_full(self):
        md = self.svc.get_quant_hub_context("600519", as_of_date="20260515")
        self.assertIsNotNone(md)
        # 基本面段
        self.assertIn("### 基本面（截至 2026-05-15）", md)
        self.assertIn("行业 白酒", md)
        self.assertIn("PE-TTM 22.30", md)
        self.assertIn("ROE 23.1%", md)
        self.assertIn("（上年年报 21.5%）", md)
        self.assertIn("毛利 75.2%", md)
        self.assertIn("营收 +12.3%", md)
        # 行情段
        self.assertIn("### 行情", md)
        self.assertIn("近 1 日 -1.23%", md)
        self.assertIn("近 5 日 +2.10%", md)
        self.assertIn("主力近 5 日", md)
        # 公司行为段
        self.assertIn("### 公司行为（近 90 日）", md)
        self.assertIn("回购 1 起", md)
        self.assertIn("增持 1 起", md)
        self.assertIn("2026-04-22 回购", md)
        self.assertIn("2026-04-15 增持 控股股东 +0.50%", md)
        # 风险段
        self.assertIn("### 风险", md)
        self.assertIn("未在 bad_stocks 黑名单", md)


class TestStaleFallback(unittest.TestCase):
    """assessment_daily 当日缺失 1 个交易日，应回溯并加陈旧标注。"""

    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        as_of = "20260515"
        calendar = ["20260511", "20260512", "20260513", "20260514", as_of]
        # 只有前一交易日有 assessment_daily 数据
        _build_root(
            self.root,
            calendar_days=calendar,
            assessment=[("20260514", [_assessment_row("600519.SH")])],
        )
        self.svc = QuantHubContextService(data_root=str(self.root), max_stale_days=3)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_fallback_to_previous_trading_day(self):
        md = self.svc.get_quant_hub_context("600519", as_of_date="20260515")
        self.assertIsNotNone(md)
        # 标注「陈旧 1 个交易日」
        self.assertIn("截至 2026-05-14，陈旧 1 个交易日", md)


class TestExceedsStaleLimit(unittest.TestCase):
    """超过 max_stale_days：对应子段跳过。"""

    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        as_of = "20260515"
        # 7 天交易日历但 assessment_daily 只有 5 天前的
        calendar = ["20260507", "20260508", "20260509", "20260512", "20260513", "20260514", as_of]
        _build_root(
            self.root,
            calendar_days=calendar,
            assessment=[("20260507", [_assessment_row("600519.SH")])],
            bad_stocks=[(as_of, [{"code": "OTHER.SH", "reason": "ST/退"}])],
        )
        self.svc = QuantHubContextService(data_root=str(self.root), max_stale_days=3)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_fundamental_skipped_but_risk_present(self):
        md = self.svc.get_quant_hub_context("600519", as_of_date="20260515")
        # 基本面段跳过，但风险段仍有
        self.assertIsNotNone(md)
        self.assertNotIn("### 基本面", md)
        self.assertIn("### 风险", md)
        self.assertIn("未在 bad_stocks 黑名单", md)


class TestNonAShare(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        _build_root(
            self.root,
            calendar_days=["20260515"],
            assessment=[("20260515", [_assessment_row("600519.SH")])],
        )
        self.svc = QuantHubContextService(data_root=str(self.root))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_hk_code_returns_none(self):
        self.assertIsNone(self.svc.get_quant_hub_context("00700"))
        self.assertIsNone(self.svc.get_quant_hub_context("hk00700"))

    def test_us_code_returns_none(self):
        self.assertIsNone(self.svc.get_quant_hub_context("AAPL"))

    def test_invalid_a_share_returns_none(self):
        # 400 前缀不在 Hub VALID_PREFIXES 里
        self.assertIsNone(self.svc.get_quant_hub_context("400123"))


class TestBadStocksHit(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        as_of = "20260515"
        _build_root(
            self.root,
            calendar_days=[as_of],
            assessment=[(as_of, [_assessment_row("000001.SZ")])],
            bad_stocks=[(as_of, [
                {"code": "000001.SZ", "reason": "risk_announcement;financial"},
                {"code": "OTHER.SH", "reason": "ST/退"},
            ])],
        )
        self.svc = QuantHubContextService(data_root=str(self.root))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_blacklist_hit(self):
        md = self.svc.get_quant_hub_context("000001", as_of_date="20260515")
        self.assertIsNotNone(md)
        self.assertIn("⚠️ 命中 bad_stocks 黑名单", md)
        self.assertIn("risk_announcement;financial", md)


class TestCaching(unittest.TestCase):
    """同一 dataset+date 在 TTL 内只读盘一次。"""

    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        as_of = "20260515"
        _build_root(
            self.root,
            calendar_days=[as_of],
            assessment=[(as_of, [_assessment_row("600519.SH"), _assessment_row("000001.SZ")])],
        )
        self.svc = QuantHubContextService(data_root=str(self.root))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_parquet_read_only_once(self):
        with patch(
            "src.services.quant_hub_context_service.pd.read_parquet",
            wraps=pd.read_parquet,
        ) as mock_read:
            self.svc.get_quant_hub_context("600519", as_of_date="20260515")
            self.svc.get_quant_hub_context("000001", as_of_date="20260515")
        # 第一次读 trade_calendar，第二次读 assessment_daily，第三次起命中缓存
        # 实际调用 ≤ 路径数 × 2（两个 code 会触发各自的 corp_actions parquet 文件检查，但这些
        # 文件不存在，所以不会真的调用 read_parquet）。
        # 我们关心的是 assessment_daily 只被读 1 次。
        paths_read = [call.args[0] for call in mock_read.call_args_list]
        assessment_reads = [p for p in paths_read if "assessment_daily" in str(p)]
        self.assertEqual(len(assessment_reads), 1,
                         f"assessment_daily 应只读一次，实际 {len(assessment_reads)}: {assessment_reads}")


class TestServiceUnavailable(unittest.TestCase):
    """data_root 不存在 / 未配置 → get_quant_hub_context 返回 None。"""

    def test_no_root(self):
        svc = QuantHubContextService(data_root=None)
        self.assertIsNone(svc.get_quant_hub_context("600519"))

    def test_missing_root(self):
        svc = QuantHubContextService(data_root="/nonexistent/quant_hub_xyz")
        self.assertIsNone(svc.get_quant_hub_context("600519"))


class TestForecastTypeScore(unittest.TestCase):
    """业绩预告档位分数 → 中文标签映射。"""

    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        as_of = "20260515"
        _build_root(
            self.root,
            calendar_days=[as_of],
            assessment=[(as_of, [_assessment_row("600519.SH", metric_d2_type_score=100)])],
        )
        self.svc = QuantHubContextService(data_root=str(self.root))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_forecast_label(self):
        md = self.svc.get_quant_hub_context("600519", as_of_date="20260515")
        self.assertIn("业绩预告: 预增", md)


if __name__ == "__main__":
    unittest.main()
