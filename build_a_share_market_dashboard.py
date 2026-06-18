# -*- coding: utf-8 -*-
"""
生成 A 股市场快讯诊断 HTML 看板。

默认行为：
  读取当前目录下的 market_dashboard_data.json，生成 a_share_market_dashboard.html。

可选行为：
  加 --refresh 时，先用当前 Python 解释器执行 a_sh_query_web.py，刷新 JSON 后再生成 HTML。

示例：
  F:\\anaconda\\Anaconda3\\python.exe build_a_share_market_dashboard.py
  F:\\anaconda\\Anaconda3\\python.exe build_a_share_market_dashboard.py --refresh
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


DEFAULT_JSON = "market_dashboard_data.json"
DEFAULT_OUTPUT = "a_share_market_dashboard.html"
DEFAULT_SOURCE_SCRIPT = "a_sh_query_web.py"


def load_report_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"未找到 JSON 文件：{path}")
    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"JSON 顶层不是对象：{path}")
    return data


def run_source_script(script_path: Path) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"--refresh 指定的数据脚本不存在：{script_path}")

    print(f"[1/3] 先运行数据脚本刷新 JSON：{script_path.name}")
    subprocess.run([sys.executable, str(script_path)], cwd=str(script_path.parent), check=True)


def html_json_dumps(data: Dict[str, Any]) -> str:
    # 防止嵌入 <script type="application/json"> 时被极端字符串提前闭合。
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def build_html(report: Dict[str, Any]) -> str:
    embedded_json = html_json_dumps(report)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    template = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>A股市场快讯诊断看板</title>
  <style>
    :root {
      --bg: #f4f7fb;
      --panel: #ffffff;
      --panel-2: #f9fbff;
      --text: #182230;
      --muted: #667085;
      --line: #e5eaf2;
      --red: #d92d20;
      --red-soft: #fff1f0;
      --green: #039855;
      --green-soft: #ecfdf3;
      --blue: #2563eb;
      --blue-soft: #eff6ff;
      --amber: #f79009;
      --amber-soft: #fffaeb;
      --purple: #7c3aed;
      --purple-soft: #f5f3ff;
      --gray-soft: #f2f4f7;
      --shadow: 0 14px 40px rgba(16, 24, 40, 0.08);
      --radius: 22px;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      background:
        radial-gradient(circle at 10% 0%, rgba(37, 99, 235, 0.13), transparent 30%),
        radial-gradient(circle at 90% 10%, rgba(124, 58, 237, 0.10), transparent 25%),
        var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", Arial, sans-serif;
      line-height: 1.5;
    }

    .page { width: min(1440px, calc(100vw - 34px)); margin: 28px auto 56px; }
    .card {
      background: rgba(255,255,255,0.94);
      border: 1px solid rgba(229, 234, 242, 0.92);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }

    .hero { display: grid; grid-template-columns: 1.55fr 1fr; gap: 18px; align-items: stretch; margin-bottom: 18px; }
    .hero-main { padding: 28px; position: relative; overflow: hidden; }
    .hero-main:after {
      content: ""; position: absolute; right: -60px; top: -80px; width: 260px; height: 260px; border-radius: 999px;
      background: linear-gradient(135deg, rgba(37, 99, 235, 0.14), rgba(124, 58, 237, 0.10));
    }
    .eyebrow { display: inline-flex; align-items: center; gap: 8px; padding: 7px 12px; border-radius: 999px; background: var(--blue-soft); color: var(--blue); font-weight: 800; font-size: 13px; margin-bottom: 14px; }
    h1 { margin: 0 0 10px; font-size: clamp(26px, 3vw, 42px); letter-spacing: -0.04em; line-height: 1.12; position: relative; z-index: 1; }
    .subtitle { margin: 0 0 18px; color: var(--muted); font-size: 15px; max-width: 850px; position: relative; z-index: 1; }

    .decision-stack { display: grid; gap: 12px; position: relative; z-index: 1; }
    .decision { display: flex; gap: 14px; align-items: flex-start; padding: 16px 18px; border-radius: 18px; border: 1px solid #fedf89; background: var(--amber-soft); }
    .decision.main { border-color: #bfdbfe; background: var(--blue-soft); }
    .decision .icon { font-size: 26px; line-height: 1; }
    .decision strong { display: block; font-size: 18px; margin-bottom: 4px; }
    .decision span { color: #6941c6; font-size: 14px; }
    .decision.main span { color: #175cd3; }

    .hero-side { padding: 22px; display: grid; gap: 12px; }
    .metric-big { padding: 18px; border-radius: 18px; background: linear-gradient(135deg, #111827, #243b67); color: white; min-height: 145px; }
    .metric-big .label { opacity: .78; font-size: 13px; }
    .metric-big .value { font-size: 38px; font-weight: 850; letter-spacing: -0.04em; margin: 6px 0; }
    .metric-big .note { opacity: .72; font-size: 13px; }
    .mini-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
    .mini-stat { padding: 12px; border-radius: 16px; background: var(--panel-2); border: 1px solid var(--line); }
    .mini-stat .label { color: var(--muted); font-size: 12px; }
    .mini-stat .value { font-size: 18px; font-weight: 850; margin-top: 4px; }

    .section { margin-top: 18px; }
    .section-head { display: flex; align-items: flex-end; justify-content: space-between; gap: 14px; margin: 26px 4px 12px; }
    .section-head h2 { margin: 0; font-size: 23px; letter-spacing: -0.02em; }
    .section-head p { margin: 4px 0 0; color: var(--muted); font-size: 14px; }
    .pill-row { display: flex; flex-wrap: wrap; gap: 8px; }
    .pill { border: 1px solid var(--line); background: white; color: var(--muted); border-radius: 999px; padding: 7px 11px; font-size: 12px; font-weight: 800; }
    .pill.blue { color: var(--blue); background: var(--blue-soft); border-color: #bfdbfe; }
    .pill.amber { color: #b54708; background: var(--amber-soft); border-color: #fedf89; }
    .pill.purple { color: var(--purple); background: var(--purple-soft); border-color: #ddd6fe; }
    .pill.green { color: var(--green); background: var(--green-soft); border-color: #abefc6; }

    .overview-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
    .index-card { padding: 18px; overflow: hidden; }
    .index-top { display: flex; justify-content: space-between; gap: 10px; align-items: flex-start; margin-bottom: 12px; }
    .index-name { font-size: 18px; font-weight: 850; }
    .index-code { color: var(--muted); font-size: 12px; margin-top: 2px; }
    .tag { border-radius: 999px; padding: 6px 10px; font-weight: 850; font-size: 12px; white-space: nowrap; }
    .tag.up { color: var(--red); background: var(--red-soft); }
    .tag.down { color: var(--green); background: var(--green-soft); }
    .tag.warn { color: #b54708; background: var(--amber-soft); }
    .tag.neutral { color: var(--blue); background: var(--blue-soft); }

    .quote-row { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; margin-bottom: 12px; }
    .quote-box { border-radius: 14px; background: var(--panel-2); border: 1px solid var(--line); padding: 10px; }
    .quote-box .label { color: var(--muted); font-size: 12px; }
    .quote-box .value { font-size: 18px; font-weight: 850; margin-top: 2px; }
    .small-chart { height: 86px; margin: 6px -4px 8px; }
    .diag-text { font-size: 13px; color: var(--muted); border-top: 1px solid var(--line); padding-top: 12px; }
    .diag-text strong { color: var(--text); }

    .chart-layout { display: grid; grid-template-columns: 1.2fr .8fr; gap: 16px; }
    .chart-card { padding: 20px; min-height: 360px; }
    .chart-title { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; margin-bottom: 12px; }
    .chart-title h3 { margin: 0; font-size: 18px; }
    .chart-title p { margin: 3px 0 0; color: var(--muted); font-size: 13px; }
    .chart-stage { width: 100%; min-height: 255px; border-radius: 18px; background: linear-gradient(180deg, #fbfdff, #f6f8fc); border: 1px solid var(--line); padding: 10px; overflow: hidden; }
    .chart-stage svg { width: 100%; height: 255px; display: block; }
    .chart-stage [data-tip], .small-chart [data-tip] { cursor: crosshair; }
    .chart-tooltip { position: fixed; z-index: 9999; max-width: 300px; padding: 9px 11px; border-radius: 12px; background: rgba(17, 24, 39, 0.94); color: #fff; font-size: 12px; line-height: 1.45; box-shadow: 0 12px 30px rgba(16, 24, 40, 0.25); pointer-events: none; transform: translate(12px, 12px); opacity: 0; transition: opacity .08s ease; white-space: pre-line; }
    .chart-tooltip.show { opacity: 1; }
    .legend { display: flex; gap: 12px; flex-wrap: wrap; color: var(--muted); font-size: 12px; margin-top: 10px; }
    .legend span { display: inline-flex; align-items: center; gap: 6px; }
    .dot { width: 10px; height: 10px; border-radius: 999px; display: inline-block; }
    .dot.blue { background: var(--blue); }
    .dot.amber { background: var(--amber); }
    .dot.purple { background: var(--purple); }
    .dot.red { background: var(--red); }
    .dot.green { background: var(--green); }

    .tabbar { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
    .tabbtn { border: 1px solid var(--line); background: white; color: var(--muted); border-radius: 999px; padding: 8px 12px; font-weight: 850; cursor: pointer; transition: .18s ease; }
    .tabbtn.active { background: #111827; color: white; border-color: #111827; }

    .north-grid { display: grid; grid-template-columns: 1fr .42fr; gap: 16px; }
    .summary-list { display: grid; gap: 10px; }
    .summary-item { padding: 14px; border-radius: 16px; background: var(--panel-2); border: 1px solid var(--line); }
    .summary-item .label { color: var(--muted); font-size: 12px; }
    .summary-item .value { margin-top: 4px; font-size: 22px; font-weight: 850; }
    .value.red { color: var(--red); }
    .value.green { color: var(--green); }
    .value.blue { color: var(--blue); }
    .value.purple { color: var(--purple); }

    .diag-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
    .deep-card { padding: 20px; }
    .level-bar { margin: 14px 0 12px; display: grid; gap: 10px; }
    .level-item { display: grid; grid-template-columns: 86px 1fr 48px; align-items: center; gap: 8px; font-size: 13px; color: var(--muted); }
    .bar-track { height: 9px; border-radius: 999px; background: #edf1f7; overflow: hidden; }
    .bar-fill { height: 100%; border-radius: 999px; background: linear-gradient(90deg, #60a5fa, #2563eb); }
    .facts { display: grid; gap: 8px; color: var(--muted); font-size: 13px; margin-top: 12px; }
    .facts b { color: var(--text); }

    .note-card { padding: 18px 20px; background: #fffdf7; border: 1px solid #fedf89; color: #704b00; border-radius: var(--radius); margin-top: 16px; }
    .note-card strong { color: #533600; }
    .footer-note { margin: 24px 4px 0; color: var(--muted); font-size: 12px; text-align: center; }
    .empty { padding: 18px; border-radius: 18px; background: var(--gray-soft); color: var(--muted); border: 1px dashed #d0d5dd; }

    @media (max-width: 1180px) {
      .hero, .chart-layout, .north-grid { grid-template-columns: 1fr; }
      .overview-grid { grid-template-columns: repeat(2, 1fr); }
      .diag-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      .page { width: min(100vw - 20px, 1440px); margin-top: 12px; }
      .overview-grid, .mini-grid { grid-template-columns: 1fr; }
      .hero-main { padding: 20px; }
      .metric-big .value { font-size: 30px; }
    }
  </style>
</head>
<body>
  <script id="market-data-json" type="application/json">__MARKET_DATA_JSON__</script>

  <div class="page">
    <section class="hero">
      <div class="card hero-main">
        <div class="eyebrow" id="eyebrow">A股市场快讯诊断看板</div>
        <h1 id="pageTitle">正在读取行情诊断数据</h1>
        <p class="subtitle" id="subtitle">数据由 a_sh_query_web.py 生成，并由本地 Python 生成器写入 HTML。页面可直接双击打开，也可后续迁移为 Web 服务读取同一份 JSON。</p>
        <div class="decision-stack">
          <div class="decision main">
            <div class="icon">📌</div>
            <div>
              <strong id="mainDecision">主判断加载中</strong>
              <span id="mainDecisionHint">主判断口径：三大指数结构 + 两市成交额 proxy。</span>
            </div>
          </div>
          <div class="decision">
            <div class="icon">⚠️</div>
            <div>
              <strong id="enhancedDecision">增强判断加载中</strong>
              <span>增强判断口径：主判断 + 北向资金辅助确认因子。</span>
            </div>
          </div>
        </div>
      </div>
      <aside class="card hero-side">
        <div class="metric-big">
          <div class="label" id="marketAmountLabel">两市成交额 proxy</div>
          <div class="value" id="marketAmountBig">-</div>
          <div class="note" id="marketAmountNote">上证成交额 + 深成指成交额；创业板只做结构参考，不重复并入。</div>
        </div>
        <div class="mini-grid">
          <div class="mini-stat"><div class="label">更新时间</div><div class="value" id="updatedAtMini">-</div></div>
          <div class="mini-stat"><div class="label">当前口径</div><div class="value" id="marketModeMini">-</div></div>
          <div class="mini-stat"><div class="label">近5日同刻</div><div class="value" id="ratio5Mini">-</div></div>
          <div class="mini-stat"><div class="label">北向确认</div><div class="value" id="northConfirmMini">-</div></div>
        </div>
      </aside>
    </section>

    <section class="section">
      <div class="section-head">
        <div>
          <h2>一眼看懂今日盘面</h2>
          <p>先看结论，再看趋势图。价格看方向，量能看确认度。</p>
        </div>
        <div class="pill-row" id="topPills"></div>
      </div>
      <div class="overview-grid" id="overviewGrid"></div>
    </section>

    <section class="section">
      <div class="section-head">
        <div>
          <h2>📈 最近交易日走势</h2>
          <p>三大指数分别看“点位趋势 + 成交额趋势”；今日盘中按同刻累计，盘后按全天口径。</p>
        </div>
      </div>
      <div class="chart-layout">
        <div class="card chart-card">
          <div class="chart-title">
            <div>
              <h3 id="trendTitle">指数 · 价格与量能</h3>
              <p id="trendDesc">价格为收盘价 / 当前点位；成交额为全天 / 今日同刻累计。</p>
            </div>
          </div>
          <div class="tabbar" id="tabbar"></div>
          <div class="chart-stage" id="priceChart"></div>
          <div class="legend"><span><i class="dot blue"></i>收盘价 / 当前点位</span><span><i class="dot amber"></i>成交额（亿元）</span></div>
          <div class="chart-stage" id="amountChart" style="margin-top:12px;"></div>
        </div>
        <div class="card chart-card">
          <div class="chart-title">
            <div>
              <h3>综合市场 · 量能趋势</h3>
              <p id="marketProxyDesc">两市成交额 proxy：上证成交额 + 深成指成交额。</p>
            </div>
          </div>
          <div class="chart-stage" id="marketAmountChart"></div>
          <div class="legend"><span><i class="dot purple"></i>两市成交额 proxy</span></div>
          <div class="note-card" id="marketNote"></div>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <div>
          <h2 id="northTitle">💴 北向资金</h2>
          <p>北向资金只取已完成交易日，不展示今日盘中实时值；它是辅助确认因子，不替代指数和量能主判断。</p>
        </div>
      </div>
      <div class="north-grid">
        <div class="card chart-card">
          <div class="chart-title">
            <div>
              <h3>北向资金净买额 / 累计净买额</h3>
              <p>柱状图看每日净买入，折线看窗口内累计变化。</p>
            </div>
            <span class="pill" id="northAsOf">截至 -</span>
          </div>
          <div class="chart-stage" id="northChart"></div>
          <div class="legend"><span><i class="dot red"></i>净买入</span><span><i class="dot green"></i>净卖出</span><span><i class="dot purple"></i>窗口累计</span></div>
        </div>
        <div class="card chart-card">
          <div class="chart-title">
            <div>
              <h3>北向资金摘要</h3>
              <p id="northSource">-</p>
            </div>
          </div>
          <div class="summary-list" id="northSummary"></div>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <div>
          <h2>🔎 三大指数深度诊断</h2>
          <p>保留原脚本里的核心诊断信息；“支撑/压力”用近20日低点/高点参考表达。</p>
        </div>
      </div>
      <div class="diag-grid" id="diagGrid"></div>
    </section>

    <div class="footer-note" id="footerNote"></div>
  </div>

  <script>
    const marketData = JSON.parse(document.getElementById('market-data-json').textContent || '{}');

    function fmt(n, digits = 2) {
      if (n === null || n === undefined || Number.isNaN(Number(n))) return "-";
      return Number(n).toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
    }
    function fmtLoose(n, digits = 2) {
      if (n === null || n === undefined || Number.isNaN(Number(n))) return "-";
      return Number(n).toLocaleString("zh-CN", { maximumFractionDigits: digits });
    }
    function pctText(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
      const sign = Number(v) >= 0 ? "+" : "";
      return sign + fmt(v, 2) + "%";
    }
    function signClass(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "neutral";
      return Number(v) >= 0 ? "up" : "down";
    }
    function valueSignClass(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "blue";
      return Number(v) >= 0 ? "red" : "green";
    }
    function safeArray(x) { return Array.isArray(x) ? x : []; }
    function shortDate(s) { return String(s || '').slice(5) || '-'; }
    function escapeHtml(s) {
      return String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
    }

    function pointsFromSeries(series, keyName) {
      return safeArray(series).map((v, i) => ({ date: String(i + 1), label: '数据点', [keyName]: v }));
    }
    function indexPricePoints(item) {
      const pts = safeArray(item.chartPoints).filter(p => p && p.price !== null && p.price !== undefined);
      if (pts.length) return pts;
      return pointsFromSeries(item.priceSeries, 'price');
    }
    function indexAmountPoints(item) {
      const pts = safeArray(item.chartPoints).filter(p => p && p.amountBillion !== null && p.amountBillion !== undefined);
      if (pts.length) return pts;
      return pointsFromSeries(item.amountSeries, 'amountBillion');
    }
    function marketAmountPoints() {
      const mp = marketData.marketProxy || {};
      const pts = safeArray(mp.amountPoints).filter(p => p && p.amountBillion !== null && p.amountBillion !== undefined);
      if (pts.length) return pts;
      return pointsFromSeries(mp.amountSeries, 'amountBillion');
    }

    function lineSvgFromPoints(points, valueKey, options = {}) {
      const valid = safeArray(points).filter(p => p && p[valueKey] !== null && p[valueKey] !== undefined && !Number.isNaN(Number(p[valueKey])));
      if (!valid.length) return '<div class="empty">暂无可用曲线数据</div>';
      const series = valid.map(p => Number(p[valueKey]));
      const w = 720, h = options.height || 230, padL = 52, padR = 22, padT = 24, padB = 36;
      const min = Math.min(...series), max = Math.max(...series), span = max - min || 1;
      const x = i => padL + i * ((w - padL - padR) / Math.max(1, series.length - 1));
      const y = v => padT + (h - padT - padB) * (1 - (v - min) / span);
      const pts = series.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
      const unit = options.unit || "";
      const metric = options.metric || "数值";
      const digits = options.digits ?? 0;
      const grid = [0, .25, .5, .75, 1].map(t => {
        const yy = padT + (h - padT - padB) * t;
        const val = max - span * t;
        return `<line x1="${padL}" y1="${yy}" x2="${w-padR}" y2="${yy}" stroke="#e5eaf2"/><text x="8" y="${yy+4}" font-size="11" fill="#667085">${fmtLoose(val, digits)}</text>`;
      }).join("");
      const labelIdx = Array.from(new Set([0, Math.floor(series.length/2), series.length - 1])).filter(i => i >= 0);
      const labels = labelIdx.map(i => `<text x="${x(i)}" y="${h-10}" text-anchor="middle" font-size="11" fill="#667085">${shortDate(valid[i].date)}</text>`).join("");
      const lastX = x(series.length-1), lastY = y(series[series.length-1]);
      const hoverPoints = valid.map((p, i) => {
        const label = `${p.date || shortDate(p.date)}｜${p.label || ''}`;
        const tip = `${label}\n${metric}：${fmt(Number(p[valueKey]), digits)}${unit}`;
        return `<circle cx="${x(i).toFixed(1)}" cy="${y(Number(p[valueKey])).toFixed(1)}" r="8" fill="transparent" stroke="transparent" data-tip="${escapeHtml(tip)}"/>`;
      }).join("");
      return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">${grid}<polyline points="${pts}" fill="none" stroke="${options.color || '#2563eb'}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/><circle cx="${lastX}" cy="${lastY}" r="5" fill="${options.color || '#2563eb'}" stroke="white" stroke-width="2"/>${hoverPoints}${labels}</svg>`;
    }

    function barSvgFromPoints(points, valueKey, options = {}) {
      const valid = safeArray(points).filter(p => p && p[valueKey] !== null && p[valueKey] !== undefined && !Number.isNaN(Number(p[valueKey])));
      if (!valid.length) return '<div class="empty">暂无可用柱状图数据</div>';
      const series = valid.map(p => Number(p[valueKey]));
      const w = 720, h = options.height || 230, padL = 52, padR = 22, padT = 24, padB = 36;
      const min0 = Math.min(0, ...series), max = Math.max(...series), span = max - min0 || 1;
      const barW = (w - padL - padR) / series.length * .68;
      const x = i => padL + i * ((w - padL - padR) / series.length) + barW * .25;
      const y = v => padT + (h - padT - padB) * (1 - (v - min0) / span);
      const zeroY = y(0);
      const metric = options.metric || "成交额";
      const unit = options.unit || " 亿";
      const digits = options.digits ?? 0;
      const grid = [0, .25, .5, .75, 1].map(t => {
        const yy = padT + (h - padT - padB) * t;
        const val = max - span * t;
        return `<line x1="${padL}" y1="${yy}" x2="${w-padR}" y2="${yy}" stroke="#e5eaf2"/><text x="8" y="${yy+4}" font-size="11" fill="#667085">${fmtLoose(val, digits)}</text>`;
      }).join("");
      const bars = valid.map((p, i) => {
        const v = Number(p[valueKey]);
        const yy = y(v);
        const hh = Math.max(2, Math.abs(zeroY - yy));
        const color = options.dualColor ? (v >= 0 ? '#d92d20' : '#039855') : (options.color || '#f79009');
        const label = `${p.date || shortDate(p.date)}｜${p.label || ''}`;
        const tip = `${label}\n${metric}：${fmt(v, digits)}${unit}`;
        return `<rect x="${x(i)}" y="${Math.min(yy, zeroY)}" width="${barW}" height="${hh}" rx="4" fill="${color}" opacity="0.86" data-tip="${escapeHtml(tip)}"/>`;
      }).join("");
      const labelIdx = Array.from(new Set([0, Math.floor(series.length/2), series.length - 1])).filter(i => i >= 0);
      const labels = labelIdx.map(i => `<text x="${padL + i*((w-padL-padR)/Math.max(1, series.length-1))}" y="${h-10}" text-anchor="middle" font-size="11" fill="#667085">${shortDate(valid[i].date)}</text>`).join("");
      return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">${grid}<line x1="${padL}" y1="${zeroY}" x2="${w-padR}" y2="${zeroY}" stroke="#98a2b3" stroke-dasharray="4 4"/>${bars}${labels}</svg>`;
    }

    function combinedNorthSvg(nb) {
      const points = safeArray(nb.netBuyPoints);
      let valid = points.filter(p => p && p.netBuyBillion !== null && p.netBuyBillion !== undefined && !Number.isNaN(Number(p.netBuyBillion)));
      if (!valid.length) {
        valid = safeArray(nb.netBuySeries).map((v, i) => ({ date: String(i + 1), label: '已完成交易日', netBuyBillion: v }));
      }
      valid = valid.filter(p => p && p.netBuyBillion !== null && p.netBuyBillion !== undefined && !Number.isNaN(Number(p.netBuyBillion)));
      if (!valid.length) return '<div class="empty">北向资金暂不可用</div>';
      let running = 0;
      const enriched = valid.map(p => {
        const v = Number(p.netBuyBillion);
        running += v;
        return { ...p, cumulativeBillion: p.cumulativeBillion ?? Number(running.toFixed(2)) };
      });
      const netSeries = enriched.map(p => Number(p.netBuyBillion));
      const cum = enriched.map(p => Number(p.cumulativeBillion));
      const w = 720, h = 255, padL = 56, padR = 24, padT = 28, padB = 36;
      const allVals = netSeries.concat(cum);
      const min = Math.min(0, ...allVals), max = Math.max(...allVals), span = max - min || 1;
      const xBar = i => padL + i * ((w-padL-padR) / netSeries.length) + 4;
      const xLine = i => padL + i * ((w-padL-padR) / Math.max(1, netSeries.length - 1));
      const y = v => padT + (h-padT-padB) * (1 - (v-min)/span);
      const zeroY = y(0);
      const barW = ((w-padL-padR)/netSeries.length)*.65;
      const grid = [0,.25,.5,.75,1].map(t => {
        const yy = padT+(h-padT-padB)*t;
        const val = max-span*t;
        return `<line x1="${padL}" y1="${yy}" x2="${w-padR}" y2="${yy}" stroke="#e5eaf2"/><text x="6" y="${yy+4}" font-size="11" fill="#667085">${fmtLoose(val,0)}</text>`;
      }).join("");
      const bars = enriched.map((p,i)=> {
        const v = Number(p.netBuyBillion);
        const action = v >= 0 ? "净买入" : "净卖出";
        const tip = `${p.date || shortDate(p.date)}｜北向资金\n${action}：${fmt(Math.abs(v), 2)} 亿\n当日净额：${fmt(v, 2)} 亿`;
        return `<rect x="${xBar(i)}" y="${Math.min(y(v), zeroY)}" width="${barW}" height="${Math.max(2, Math.abs(zeroY-y(v)))}" rx="4" fill="${v>=0 ? '#d92d20' : '#039855'}" opacity=".82" data-tip="${escapeHtml(tip)}"/>`;
      }).join("");
      const pts = cum.map((v,i)=> `${xLine(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
      const hoverPoints = enriched.map((p,i)=> {
        const tip = `${p.date || shortDate(p.date)}｜北向资金\n窗口内累计净买额：${fmt(Number(p.cumulativeBillion), 2)} 亿`;
        return `<circle cx="${xLine(i).toFixed(1)}" cy="${y(Number(p.cumulativeBillion)).toFixed(1)}" r="8" fill="transparent" stroke="transparent" data-tip="${escapeHtml(tip)}"/>`;
      }).join("");
      const labelIdx = Array.from(new Set([0, Math.floor(netSeries.length/2), netSeries.length-1])).filter(i => i >= 0);
      const labels = labelIdx.map(i=> `<text x="${xLine(i)}" y="${h-10}" text-anchor="middle" font-size="11" fill="#667085">${shortDate(enriched[i].date)}</text>`).join("");
      return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">${grid}<line x1="${padL}" y1="${zeroY}" x2="${w-padR}" y2="${zeroY}" stroke="#98a2b3" stroke-dasharray="4 4"/>${bars}<polyline points="${pts}" fill="none" stroke="#7c3aed" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>${hoverPoints}${labels}</svg>`;
    }

    function cleanDecisionTitle(decision, structureTag) {
      let text = String(decision || '').replace(/^⚠️\\s*/, '').replace(/^✅\\s*/, '').replace(/^❌\\s*/, '').trim();
      // 盘中如果上证偏弱、创业板明显强，标题不要过度写成“三大指数共振”。
      if (/共振/.test(text) && /创业板明显强于上证|分化|成长线/.test(String(structureTag || ''))) {
        return text.replace('放量共振走强', '创业板带动市场放量走强').replace('指数共振走强', '成长线带动市场走强');
      }
      return text || 'A股市场快讯诊断看板';
    }

    function compactEnhancedDecision(decision, nb) {
      const base = String(decision || '-');
      if (!nb || !nb.available) return base;

      const shortPart = nb.sum5Billion === null || nb.sum5Billion === undefined
        ? ''
        : `近5日${Number(nb.sum5Billion) >= 0 ? '净流入' : '净流出'}`;
      const windowValue = nb.sumAvailableBillion ?? nb.sum20Billion;
      const days = nb.actualDays || nb.requestedLookbackDays || '';
      const windowPart = windowValue === null || windowValue === undefined
        ? ''
        : `近${days || ''}个交易日${Number(windowValue) >= 0 ? '净流入' : '净流出'}`;
      const confirm = nb.confirmTag || '北向资金确认度待观察';

      const flowParts = [shortPart, windowPart].filter(Boolean);
      if (!flowParts.length) return `${base}；同时${confirm}，仍需观察资金回流。`;
      return `${base}；同时北向资金${flowParts.join('、')}，${confirm}，仍需观察资金回流。`;
    }

    function renderHero() {
      const meta = marketData.meta || {};
      const finalObj = marketData.final || {};
      const mp = marketData.marketProxy || {};
      const nb = marketData.northbound || {};
      const updatedAt = meta.updatedAt || marketData.updatedAt || '-';
      const isClosed = !!(meta.isMarketClosed || marketData.isMarketClosed);
      const mode = isClosed ? '收盘口径' : '盘中同刻';
      const decision = finalObj.decision || marketData.finalDecision || '-';
      const enhanced = compactEnhancedDecision(decision, nb);
      const displayAmount = !isClosed && mp.sameTimeBillion !== null && mp.sameTimeBillion !== undefined ? mp.sameTimeBillion : mp.amountBillion;
      const amountLabel = !isClosed ? '两市同刻累计 proxy' : '两市成交额 proxy';
      const amountNote = !isClosed
        ? `同刻累计用于量能比较；当前快照金额：${fmt(mp.amountBillion, 2)} 亿；同刻累计金额：${fmt(mp.sameTimeBillion, 2)} 亿。${mp.description || ''}`
        : (mp.description || '上证成交额 + 深成指成交额；创业板只做结构参考，不重复并入。');

      const displayTitle = cleanDecisionTitle(decision, finalObj.structureTag);
      const rawTitle = String(decision || '').replace(/^⚠️\\s*/, '').replace(/^✅\\s*/, '').replace(/^❌\\s*/, '').trim();
      const titleWasAdjusted = displayTitle && rawTitle && displayTitle !== rawTitle;

      document.getElementById('eyebrow').textContent = `A股市场快讯诊断看板 · ${updatedAt}`;
      document.getElementById('pageTitle').textContent = displayTitle;
      document.getElementById('mainDecision').textContent = decision;
      document.getElementById('mainDecisionHint').textContent = titleWasAdjusted
        ? '主判断为原始脚本结论；页面大标题已结合指数结构做更直观表达。'
        : '主判断口径：三大指数结构 + 两市成交额 proxy。';
      document.getElementById('enhancedDecision').textContent = enhanced;
      document.getElementById('marketAmountLabel').textContent = amountLabel;
      document.getElementById('marketAmountBig').textContent = `${fmt(displayAmount, 2)} 亿`;
      document.getElementById('marketAmountNote').textContent = amountNote;
      document.getElementById('updatedAtMini').textContent = updatedAt;
      document.getElementById('marketModeMini').textContent = mode;
      document.getElementById('ratio5Mini').textContent = mp.ratio5 === null || mp.ratio5 === undefined ? '-' : `${fmt(mp.ratio5, 2)}x`;
      document.getElementById('northConfirmMini').textContent = nb.confirmTag || (nb.available ? '北向可用' : '暂不可用');
      document.getElementById('topPills').innerHTML = `
        <span class="pill blue">${meta.currentPointLabel || marketData.currentPointLabel || mode}</span>
        <span class="pill amber">两市量能：${finalObj.volumeTag || mp.volumeTag || '-'}</span>
        <span class="pill purple">指数结构：${finalObj.structureTag || '-'}</span>
        <span class="pill green">${nb.title || '北向资金'}</span>
        <span class="pill">数据已嵌入 JSON</span>`;
    }

    function renderOverview() {
      const indexes = safeArray(marketData.indexes).filter(x => x && x.ok !== false);
      const mp = marketData.marketProxy || {};
      const cards = indexes.map(item => ({ type: 'index', ...item }));
      const isClosed = !!((marketData.meta || {}).isMarketClosed || marketData.isMarketClosed);
      const proxyAmountForDisplay = !isClosed && mp.sameTimeBillion !== null && mp.sameTimeBillion !== undefined ? mp.sameTimeBillion : mp.amountBillion;
      cards.push({
        type: 'marketProxy', code: 'market_proxy', name: isClosed ? (mp.name || '两市成交额 proxy') : '两市同刻累计 proxy', last: proxyAmountForDisplay,
        pct: null, amountBillion: proxyAmountForDisplay, diagTag: mp.volumeTag || '两市量能', volumeTag: mp.volumeTag || '两市量能',
        chartPoints: marketAmountPoints(), amountSeries: mp.amountSeries
      });

      document.getElementById('overviewGrid').innerHTML = cards.map(item => {
        const isProxy = item.type === 'marketProxy';
        const overviewAmountLabel = isProxy ? (isClosed ? '当前成交额' : '同刻累计额') : '点位';
        const spark = isProxy
          ? barSvgFromPoints(marketAmountPoints(), 'amountBillion', {height:86, color:'#7c3aed', digits:0, metric:isClosed ? '两市成交额 proxy' : '两市同刻累计 proxy', unit:' 亿'})
          : lineSvgFromPoints(indexPricePoints(item), 'price', {height:86, color:'#2563eb', digits:0, metric:'点位', unit:' 点'});
        return `<article class="card index-card">
          <div class="index-top">
            <div><div class="index-name">${escapeHtml(item.name)}</div><div class="index-code">${escapeHtml(item.code || '')}</div></div>
            <span class="tag ${isProxy ? 'neutral' : signClass(item.pct)}">${isProxy ? '量能' : pctText(item.pct)}</span>
          </div>
          <div class="quote-row">
            <div class="quote-box"><div class="label">${overviewAmountLabel}</div><div class="value">${fmt(isProxy ? item.amountBillion : item.last,2)}${isProxy ? ' 亿' : ''}</div></div>
            <div class="quote-box"><div class="label">${isProxy ? '近20日同刻' : '成交额'}</div><div class="value">${isProxy ? ((mp.ratio20 === null || mp.ratio20 === undefined) ? '-' : fmt(mp.ratio20,2)+'x') : fmt(item.amountBillion,2)+' 亿'}</div></div>
          </div>
          <div class="small-chart">${spark}</div>
          <div class="diag-text"><strong>${escapeHtml(item.volumeTag || item.diagTag || '')}</strong><br>${escapeHtml(item.diagTag || '')}</div>
        </article>`;
      }).join('');
    }

    function renderTabs() {
      const tabItems = safeArray(marketData.indexes).filter(x => x && x.ok !== false);
      const tabbar = document.getElementById('tabbar');
      if (!tabItems.length) {
        tabbar.innerHTML = '';
        document.getElementById('priceChart').innerHTML = '<div class="empty">暂无指数图表数据</div>';
        document.getElementById('amountChart').innerHTML = '<div class="empty">暂无指数成交额数据</div>';
        return;
      }
      tabbar.innerHTML = tabItems.map((item, i) => `<button class="tabbtn ${i===0?'active':''}" data-i="${i}">${escapeHtml(item.name)}</button>`).join('');
      function show(i) {
        tabbar.querySelectorAll('.tabbtn').forEach((btn, idx) => btn.classList.toggle('active', idx === i));
        const item = tabItems[i];
        document.getElementById('trendTitle').textContent = `${item.name} · 价格与量能`;
        document.getElementById('trendDesc').textContent = '价格为收盘价 / 当前点位；成交额为全天 / 今日同刻累计。';
        document.getElementById('priceChart').innerHTML = lineSvgFromPoints(indexPricePoints(item), 'price', {color:'#2563eb', digits:0, metric:'点位', unit:' 点'});
        document.getElementById('amountChart').innerHTML = barSvgFromPoints(indexAmountPoints(item), 'amountBillion', {color:'#f79009', digits:0, metric:'成交额', unit:' 亿'});
      }
      tabbar.addEventListener('click', e => { if (e.target.matches('.tabbtn')) show(Number(e.target.dataset.i)); });
      show(0);
    }

    function renderMarketAndNorth() {
      const mp = marketData.marketProxy || {};
      document.getElementById('marketProxyDesc').textContent = mp.description || '两市成交额 proxy：上证成交额 + 深成指成交额。';
      document.getElementById('marketAmountChart').innerHTML = barSvgFromPoints(marketAmountPoints(), 'amountBillion', {color:'#7c3aed', digits:0, metric:'两市成交额 proxy', unit:' 亿'});
      const r5 = mp.ratio5 === null || mp.ratio5 === undefined ? '-' : `${fmt(mp.ratio5,2)}x`;
      const r20 = mp.ratio20 === null || mp.ratio20 === undefined ? '-' : `${fmt(mp.ratio20,2)}x`;
      document.getElementById('marketNote').innerHTML = `<strong>为什么先看两市成交额？</strong><br>指数上涨如果没有成交额配合，持续性通常要打折。当前近5日同刻约 ${r5}，近20日同刻约 ${r20}。两市成交额 proxy 用于观察全市场活跃度，不把创业板重复并入。`;

      const nb = marketData.northbound || {};
      document.getElementById('northTitle').textContent = `💴 ${nb.title || '北向资金'}`;
      document.getElementById('northAsOf').textContent = nb.asOf ? `截至 ${nb.asOf}` : '暂不可用';
      document.getElementById('northSource').textContent = nb.available ? `来源：${nb.source || '-'}；${nb.role || '辅助确认因子'}` : (nb.note || '北向资金暂不可用');
      document.getElementById('northChart').innerHTML = combinedNorthSvg(nb);
      renderNorthSummary(nb);
    }

    function moneyText(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return '-';
      const sign = Number(v) > 0 ? '+' : '';
      return `${sign}${fmt(Number(v), 2)} 亿`;
    }
    function renderNorthSummary(nb) {
      if (!nb || !nb.available) {
        document.getElementById('northSummary').innerHTML = `<div class="summary-item"><div class="label">状态</div><div class="value blue" style="font-size:16px;line-height:1.55;">${escapeHtml(nb && nb.note ? nb.note : '北向资金暂不可用')}</div></div>`;
        return;
      }
      const items = [
        ['最近1日', moneyText(nb.lastNetBuyBillion), valueSignClass(nb.lastNetBuyBillion)],
        ['最近5日合计', moneyText(nb.sum5Billion), valueSignClass(nb.sum5Billion)],
        ['窗口累计', moneyText(nb.sumAvailableBillion ?? nb.sum20Billion), valueSignClass(nb.sumAvailableBillion ?? nb.sum20Billion)],
        ['窗口日均', moneyText(nb.meanBillion), valueSignClass(nb.meanBillion)],
        ['最新日 z-score', nb.zScoreLatest === null || nb.zScoreLatest === undefined ? '-' : fmt(nb.zScoreLatest, 2), 'purple'],
        ['确认度', nb.confirmTag || '-', 'blue'],
      ];
      const html = items.map(([label, value, cls]) => `<div class="summary-item"><div class="label">${label}</div><div class="value ${cls}">${value}</div></div>`).join('');
      const explain = `<div class="summary-item"><div class="label">解读</div><div class="value" style="font-size:16px;line-height:1.55;">${escapeHtml(nb.explain || nb.note || '-')}</div></div>`;
      document.getElementById('northSummary').innerHTML = html + explain;
    }

    function renderDiag() {
      const indexes = safeArray(marketData.indexes).filter(x => x && x.ok !== false);
      document.getElementById('diagGrid').innerHTML = indexes.map(item => `
        <article class="card deep-card">
          <div class="index-top">
            <div><div class="index-name">${escapeHtml(item.name)}</div><div class="index-code">${escapeHtml(item.code || '')} · ${escapeHtml(item.source || item.quoteSource || '')}</div></div>
            <span class="tag warn">${escapeHtml(item.volumeTag || '量能状态')}</span>
          </div>
          <div class="quote-row">
            <div class="quote-box"><div class="label">点位 / 涨跌</div><div class="value">${fmt(item.last,2)} · ${pctText(item.pct)}</div></div>
            <div class="quote-box"><div class="label">当前成交额</div><div class="value">${fmt(item.amountBillion,2)} 亿</div></div>
          </div>
          <div class="level-bar">
            <div class="level-item"><span>近5日量能</span><div class="bar-track"><div class="bar-fill" style="width:${Math.min((Number(item.ratio5)||0)*70, 100)}%"></div></div><b>${item.ratio5 === null || item.ratio5 === undefined ? '-' : fmt(item.ratio5,2)+'x'}</b></div>
            <div class="level-item"><span>近20日量能</span><div class="bar-track"><div class="bar-fill" style="width:${Math.min((Number(item.ratio20)||0)*70, 100)}%"></div></div><b>${item.ratio20 === null || item.ratio20 === undefined ? '-' : fmt(item.ratio20,2)+'x'}</b></div>
            <div class="level-item"><span>昨日同刻</span><div class="bar-track"><div class="bar-fill" style="width:${Math.min((Number(item.ratioYday)||0)*70, 100)}%"></div></div><b>${item.ratioYday === null || item.ratioYday === undefined ? '-' : fmt(item.ratioYday,2)+'x'}</b></div>
          </div>
          <div class="facts">
            <div>均线：MA5 <b>${fmt(item.ma5,2)}</b>，MA10 <b>${fmt(item.ma10,2)}</b>，MA20 <b>${fmt(item.ma20,2)}</b></div>
            <div>近20日区间：低点参考 <b>${fmt(item.low20,2)}</b>，高点参考 <b>${fmt(item.high20,2)}</b></div>
            <div>K线：<b>${escapeHtml(item.kline || '-')}</b></div>
            <div>结论：<b>${escapeHtml(item.diagTag || '-')}</b></div>
          </div>
        </article>
      `).join('');
    }

    function setupChartTooltip() {
      const tip = document.createElement('div');
      tip.className = 'chart-tooltip';
      document.body.appendChild(tip);
      document.addEventListener('mousemove', (e) => {
        const target = e.target.closest('[data-tip]');
        if (!target) { tip.classList.remove('show'); return; }
        tip.textContent = target.getAttribute('data-tip');
        tip.style.left = `${e.clientX}px`;
        tip.style.top = `${e.clientY}px`;
        tip.classList.add('show');
      });
      document.addEventListener('mouseleave', () => tip.classList.remove('show'));
    }

    function renderFooter() {
      const meta = marketData.meta || {};
      document.getElementById('footerNote').textContent = `数据状态：已嵌入 market_dashboard_data.json；本页面由 build_a_share_market_dashboard.py 生成；HTML生成时间：__GENERATED_AT__；数据更新时间：${meta.updatedAt || marketData.updatedAt || '-'}。仅供市场观察与辅助判断，不构成投资建议。`;
    }

    function renderAll() {
      renderHero();
      renderOverview();
      renderTabs();
      renderMarketAndNorth();
      renderDiag();
      renderFooter();
      setupChartTooltip();
    }

    renderAll();
  </script>
</body>
</html>
'''

    return template.replace("__MARKET_DATA_JSON__", embedded_json).replace("__GENERATED_AT__", generated_at)


def write_html(html: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp_{os.getpid()}")
    with tmp.open("w", encoding="utf-8-sig", newline="\n") as f:
        f.write(html)
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="读取 market_dashboard_data.json 并生成 a_share_market_dashboard.html")
    parser.add_argument("--json", default=DEFAULT_JSON, help="输入 JSON 文件，默认 market_dashboard_data.json")
    parser.add_argument("--out", default=DEFAULT_OUTPUT, help="输出 HTML 文件，默认 a_share_market_dashboard.html")
    parser.add_argument("--refresh", action="store_true", help="生成 HTML 前先运行 a_sh_query_web.py 刷新 JSON")
    parser.add_argument("--source", default=DEFAULT_SOURCE_SCRIPT, help="--refresh 时执行的数据脚本，默认 a_sh_query_web.py")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    json_path = (base_dir / args.json).resolve() if not os.path.isabs(args.json) else Path(args.json).resolve()
    out_path = (base_dir / args.out).resolve() if not os.path.isabs(args.out) else Path(args.out).resolve()
    source_path = (base_dir / args.source).resolve() if not os.path.isabs(args.source) else Path(args.source).resolve()

    try:
        if args.refresh:
            run_source_script(source_path)
        print(f"[2/3] 读取 JSON：{json_path}")
        report = load_report_json(json_path)
        print(f"[3/3] 生成 HTML：{out_path}")
        html = build_html(report)
        write_html(html, out_path)
        print("Done.")
        print(f"输出文件：{out_path}")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"ERROR: 数据脚本执行失败，返回码={e.returncode}", file=sys.stderr)
        return e.returncode or 1
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
