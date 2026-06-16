# -*- coding: utf-8 -*-

"""
a_sh_query_web.py — 最终整合版（s_ 同步点位+涨跌幅+当日金额 · 覆盖即用 · 含当日口径校准因子k）

* 历史分钟：pytdx（指数 get_index_bars，个股 get_security_bars），容错清洗 + 单位归一
* 当日兜底：Wind/QQ 分时用于当日补尾（Wind主源，QQ兜底；指数金额优先Wind快照）
* 成交额口径：Wind主源成交额；失败回退腾讯简要行情 s_ / 分钟累计
* 点位口径：s_【最新点位/涨跌幅】；失败再退 QQ/TDX
* 分析口径“一把尺”：盘中同刻用 k 对齐；盘后直接用Wind主源口径覆盖“今日全日金额”
* 展示一致性：同刻口径（展示）一律用 k_close 对齐Wind主源（不改判定逻辑）
* 缓存：cache/minutes/{code}/{YYYYMMDD}.parquet（按日；失败降级 CSV）

本版集成 6 个补丁 + 5 个微调，并“补回”两个同刻指标（仅展示，不影响判定）：
【原 6 补丁】
1. 权威金额缓存按“(code, day)”隔离 + TTL（None=10s，有效=60s），稳定器也按日；支持从 None 恢复
2. intraday_amt_ratio 窗口真正使用 window_days，样本不足逐级降级到 ≥3
3. s_ 涨跌幅提取：优先按常位索引，失败再全局扫描，降低误取概率
4. 收盘判定边界：统一使用 >= "14:59" 作为收盘判断，更稳
5. 放量判定阈值：强放量改为“两项≥1.10x 或 单项≥1.30x”，中间档含 0.95~1.10 的灰区
6. 交易分钟过滤：当 vol/amount 覆盖率低时不强行过滤，仅按时段过滤；否则启用过滤

【新增 5 微调】
A) 快讯中当 s_ 无涨跌幅时，用前收回算涨跌幅填充
B) 结论理由按“前缀”去重，避免同义重复
C) 盘后在“同刻口径”下方增加一条口径说明文案
D) 交易分钟过滤的有效覆盖率阈值从 0.2 提到 0.3，更稳
E) s_ 金额字段回退命中时打印更详细日志（注明从 idx7 回退到哪个 idx）

【补回 2 个同刻展示指标（不改变判定）】
Y1) 昨日同刻累计额（展示） 及 “今日/昨日同刻倍数”
Y2) 近3日同刻均额（展示） 及 “今日/近3日同刻倍数”
"""

import os, time, json, math, random, subprocess
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, List

import pandas as pd
import requests
from pytdx.hq import TdxHq_API

# ====== 参数 ======

INDEX_CODES = ["sh000001", "sz399001", "sz399006"]
MAIN_CODE   = "sh000001"
FREQ        = "1m"
HIST_START  = None
HIST_END    = None
WINDOW_DAYS = 5
CACHE_DIR   = "cache/minutes"
TIMEOUT_SEC = 2.5
CACHE_WRITE_COOLDOWN = int(os.environ.get("CACHE_WRITE_COOLDOWN", "60"))

# ====== Wind MCP 主源配置（新增，失败自动回落原免费源）======
# USE_WIND=0 可临时关闭 Wind 主源，回到原腾讯/QQ/TDX 逻辑
USE_WIND = os.environ.get("USE_WIND", "1") != "0"

# 优先使用你本机已经验证成功的新 Node，避免误命中旧 Node 14 导致 fetch is not defined
WIND_NODE_EXE = os.environ.get("WIND_NODE_EXE", r"D:\node\node.exe")
WIND_SKILL_DIR = os.environ.get(
    "WIND_SKILL_DIR",
    os.path.join(os.path.expanduser("~"), ".agents", "skills", "wind-mcp-skill")
)
WIND_CLI = os.path.join(WIND_SKILL_DIR, "scripts", "cli.mjs")
WIND_TIMEOUT_SEC = float(os.environ.get("WIND_TIMEOUT_SEC", "12"))

# 本脚本内部代码 -> Wind 代码
WIND_INDEX_CODE_MAP = {
    "sh000001": "000001.SH",
    "sz399001": "399001.SZ",
    "sz399006": "399006.SZ",
}


# ====== 调试开关（新增）======
# 打印“昨日同刻 / 近3日同刻”两项展示值的来源与关键中间量
# True：打印；False：关闭
DEBUG_SAME_TIME_LOG = os.environ.get("DEBUG_SAME_TIME_LOG", "0") == "1"

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/126 Chrome/126 Safari/537.36",
]

PAGE_SIZE = 800
CAT_MAP = {"1m":8, "5m":9, "15m":10, "30m":11, "60m":12}
INDEX_WHITELIST_SH = {"000001","000016","000300","000905","000852","000688","000010","000011","000012"}

Q_MKLINE_ENDPOINTS = [
    "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/kline/mkline",
    "https://web.ifzq.gtimg.cn/appstock/app/kline/mkline",
]
Q_SIMPLE_QUOTE = "https://qt.gtimg.cn/q="  # q=s_sh000001

# 是否打印北向资金（网页端无权威盘中，默认 False）
SHOW_NORTHBOUND = False

CST = timezone(timedelta(hours=8))
_PARQUET_WARNED_ONCE = False

# ====== Wind MCP 主源（新增，失败自动回落原免费源）======

_WIND_WARNED = set()


def _wind_warn_once(key: str, msg: str):
    # Wind 报错只提示一次，避免刷屏。
    if key in _WIND_WARNED:
        return
    _WIND_WARNED.add(key)
    print(msg)


def _wind_float(x):
    if x is None:
        return None
    try:
        s = str(x).strip()
        if not s or s.upper() == "INVALID" or s.lower() == "null":
            return None
        v = float(s.replace(",", "").replace("%", ""))
        return v if math.isfinite(v) else None
    except Exception:
        return None


def _wind_table_to_rows(data: dict) -> List[dict]:
    # Wind MCP 返回结构一般是：
    # data.columns = [{"name": "..."}]
    # data.rows    = [[...], [...]]
    # 这里统一转成 list[dict]，方便后续取字段。
    if not isinstance(data, dict):
        return []

    cols_raw = data.get("columns") or []
    rows_raw = data.get("rows") or []

    cols = []
    for c in cols_raw:
        if isinstance(c, dict):
            cols.append(c.get("name"))
        else:
            cols.append(str(c))

    rows = []
    for r in rows_raw:
        item = {}
        for i, col in enumerate(cols):
            if not col:
                continue
            item[col] = r[i] if i < len(r) else None
        rows.append(item)
    return rows


def _call_wind_mcp(server_type: str, tool_name: str, params: dict) -> Optional[dict]:
    # Python -> D:\node\node.exe -> wind-mcp-skill/scripts/cli.mjs -> Wind
    # 任何异常都返回 None，让外层自动回落原逻辑。
    if not USE_WIND:
        return None

    if not os.path.exists(WIND_NODE_EXE):
        _wind_warn_once("node_missing", f"[wind] 找不到 Node：{WIND_NODE_EXE}，已回落原数据源")
        return None

    if not os.path.exists(WIND_CLI):
        _wind_warn_once("cli_missing", f"[wind] 找不到 Wind CLI：{WIND_CLI}，已回落原数据源")
        return None

    try:
        params_json = json.dumps(params, ensure_ascii=False)
        cmd = [
            WIND_NODE_EXE,
            WIND_CLI,
            "call",
            server_type,
            tool_name,
            params_json,
        ]

        p = subprocess.run(
            cmd,
            cwd=WIND_SKILL_DIR,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=WIND_TIMEOUT_SEC,
        )

        if p.returncode != 0:
            _wind_warn_once(
                f"cmd_fail_{server_type}_{tool_name}",
                f"[wind] 调用失败：{server_type}.{tool_name}，已回落原数据源；err={p.stderr.strip()[:300]}"
            )
            return None

        outer = json.loads(p.stdout)
        if outer.get("isError"):
            _wind_warn_once(
                f"outer_error_{server_type}_{tool_name}",
                f"[wind] 返回 isError：{server_type}.{tool_name}，已回落原数据源"
            )
            return None

        content = outer.get("content") or []
        if not content:
            return None

        inner_text = content[0].get("text")
        if not inner_text:
            return None

        inner = json.loads(inner_text)
        if inner.get("error"):
            _wind_warn_once(
                f"inner_error_{server_type}_{tool_name}",
                f"[wind] 内层返回 error：{server_type}.{tool_name}，已回落原数据源"
            )
            return None

        return inner
    except Exception as e:
        _wind_warn_once(
            f"exception_{server_type}_{tool_name}",
            f"[wind] 异常：{server_type}.{tool_name}，已回落原数据源；err={repr(e)[:300]}"
        )
        return None


def fetch_wind_index_snapshot(code_std: str) -> Optional[dict]:
    # Wind 指数快照：
    #     最新成交价 -> last
    #     涨跌幅     -> pct
    #     成交额     -> amt_yuan，单位：元
    # 返回结构伪装成原 fetch_index_simple_quote() 的结构，主流程无需改。
    windcode = WIND_INDEX_CODE_MAP.get(code_std)
    if not windcode:
        return None

    inner = _call_wind_mcp(
        "index_data",
        "get_index_price_indicators",
        {
            "windcode": windcode,
            "indexes": "最新成交价,涨跌幅,成交量,成交额",
        },
    )
    if not inner:
        return None

    rows = _wind_table_to_rows(inner.get("data") or {})
    if not rows:
        return None

    row = rows[0]
    last = _wind_float(row.get("最新成交价"))
    pct = _wind_float(row.get("涨跌幅"))
    amt_yuan = _wind_float(row.get("成交额"))

    if last is None and pct is None and amt_yuan is None:
        return None

    # 复用原来的权威金额稳定器，避免金额异常回跳
    amt_yuan = _stable_authoritative(code_std, amt_yuan)

    return {
        "last": last,
        "pct": pct,
        "amt_yuan": amt_yuan,
        "source": "wind_snapshot",
        "quote_source": "wind_snapshot",
        "amount_source": "wind_snapshot",
    }


def fetch_wind_today_minutes(code_std: str) -> pd.DataFrame:
    # Wind 今日分钟行情：
    #     TIME     -> datetime / date / time
    #     MATCH    -> close
    #     VOLUME   -> vol
    #     TURNOVER -> amount，单位：元，且为每分钟成交额增量
    # 注意：Wind 的 TURNOVER 已实测：全日分钟求和 = 快照成交额。
    windcode = WIND_INDEX_CODE_MAP.get(code_std)
    if not windcode:
        return pd.DataFrame()

    inner = _call_wind_mcp(
        "index_data",
        "get_index_quote",
        {
            "windcode": windcode,
        },
    )
    if not inner:
        return pd.DataFrame()

    rows = _wind_table_to_rows(inner.get("data") or {})
    if not rows:
        return pd.DataFrame()

    out = []
    today = today_str_cst()

    for r in rows:
        ts_raw = r.get("TIME")
        ts = pd.to_datetime(ts_raw, errors="coerce")
        if pd.isna(ts):
            continue

        # Wind 返回类似 2026-06-15T09:30:00.000+08:00
        # 这里去掉时区信息但保留本地 09:30，不转 UTC。
        try:
            if getattr(ts, "tzinfo", None) is not None:
                ts = ts.tz_localize(None)
        except Exception:
            pass

        date_str = ts.strftime("%Y-%m-%d")
        if date_str != today:
            continue

        close = _wind_float(r.get("MATCH"))
        if close is None:
            continue

        vol = _wind_float(r.get("VOLUME"))
        amt = _wind_float(r.get("TURNOVER"))

        out.append({
            "code": code_std,
            "date": date_str,
            "time": ts.strftime("%H:%M"),
            "hhmm": ts.strftime("%H:%M"),
            "datetime": ts.to_pydatetime(),
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "vol": vol if vol is not None else 0.0,
            "amount": amt if amt is not None else 0.0,
            "_amount_estimated": False,
            "_patch_source": "wind_minutes",
        })

    if not out:
        return pd.DataFrame()

    df = pd.DataFrame(out)
    df = df.drop_duplicates(subset=["code", "datetime"]).sort_values("datetime").reset_index(drop=True)
    return df


# ====== 工具 ======

def ensure_dir(p: str): os.makedirs(p, exist_ok=True)
def today_str_cst() -> str: return datetime.now(CST).date().strftime("%Y-%m-%d")

def readable_billion_from_yuan(x: float) -> str:
    if x is None or not isinstance(x, (int, float)) or not math.isfinite(x):
        return "-"
    return f"{x/1e8:.2f} 亿"

def fmt_ratio(r: Optional[float]) -> str:
    if r is None or not math.isfinite(r) or r <= 0:
        return "N/A"
    return f"{r:.2f}x"


# ====== 数据源标签（用于展示真实来源） ======

SOURCE_LABELS = {
    "wind_snapshot": "Wind主源快照",
    "tencent_s": "腾讯s_简要行情",
    "tencent_s_direct": "腾讯s_直连兜底",
    "wind_minutes": "Wind分钟补尾",
    "qq_minutes": "QQ分钟补尾",
    "tdx": "TDX历史分钟",
    "minute_sum": "分钟累计",
    "today_patch": "今日补尾",
}


def _source_label(src: Optional[str]) -> str:
    if not src:
        return "未知来源"
    return SOURCE_LABELS.get(src, src)

def _parse_code_market(code: str):
    s = code.lower().strip()
    if s.startswith("sh"): return 1, s[2:], "sh"+s[2:]
    if s.startswith("sz"): return 0, s[2:], "sz"+s[2:]
    if s.startswith(("5","6","9")): return 1, s, "sh"+s
    return 0, s, "sz"+s

def _is_index(market: int, pure: str) -> bool:
    if market == 0 and pure.startswith("399"): return True
    if market == 1 and pure in INDEX_WHITELIST_SH: return True
    return False

def _best_server(api: TdxHq_API):
    try:
        best = api.best_ip()
        if best and "ip" in best and "port" in best:
            return best["ip"], best["port"]
    except Exception:
        pass
    pool = [("180.153.18.170",7709),("180.153.18.171",7709),
            ("119.147.212.81",7709),("119.147.212.83",7709),
            ("119.147.171.206",7709),("14.17.75.71",7709),
            ("218.108.98.244",7709),("218.108.47.69",7709)]
    random.shuffle(pool)
    for ip, port in pool:
        try:
            if api.connect(ip, port):
                api.disconnect()
                return ip, port
        except Exception:
            pass
    return pool[0]

def _clean_price_rows(df: pd.DataFrame, is_index: bool) -> pd.DataFrame:
    if df.empty: return df
    cols = [c for c in ["open","close","high","low"] if c in df.columns]
    for c in cols: df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in cols: df = df[df[c] > 0]
    if is_index:
        for c in cols: df = df[(df[c] >= 500) & (df[c] <= 20000)]
    else:
        for c in cols: df = df[(df[c] >= 0.1) & (df[c] <= 100000)]
    return df

def _ensure_amount(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    for c in ["open","high","low","close","vol"]:
        if c not in df.columns:
            df[c] = pd.NA
    df["_amount_estimated"] = False

    need_est = ("amount" not in df.columns) or df["amount"].isna().all()
    if not need_est: return df

    o = pd.to_numeric(df["open"], errors="coerce")
    h = pd.to_numeric(df["high"], errors="coerce")
    l = pd.to_numeric(df["low"],  errors="coerce")
    c = pd.to_numeric(df["close"],errors="coerce")
    v = pd.to_numeric(df["vol"],  errors="coerce")
    vwap = (o + 2*c + h + l) / 5.0
    est_mask = v.notna() & vwap.notna()
    df.loc[est_mask, "amount"] = (vwap * v)[est_mask]
    df.loc[est_mask, "_amount_estimated"] = True
    return df

def _bars_for_minutes(freq: str, minutes: int) -> int:
    unit = {"1m":1, "5m":5, "15m":15, "30m":30, "60m":60}.get(freq, 1)
    return max(1, minutes // unit)

def _normalize_index_amount_units(df_idx: pd.DataFrame, code_std: str) -> pd.DataFrame:
    # [PATCH 5] 单位判别先去极端噪声再取中位数
    if df_idx.empty or "amount" not in df_idx.columns: return df_idx
    s = pd.to_numeric(df_idx["amount"], errors="coerce")
    s_valid = s[(s > 0) & (s < s.quantile(0.995))]
    if s_valid.dropna().empty: return df_idx
    med = float(s_valid.median())
    scale = 1.0
    if med > 1e11: scale = 1/1e4
    elif med < 1e6: scale = 1e4
    df_idx["amount"] = s * scale
    if scale != 1.0:
        print(f"[normalize] 检测到指数金额单位异常，已应用 scale={scale}")
    return df_idx

# ====== s_ 简要行情 ======

# [PATCH 1] 增加 TTL 缓存，2 秒过期
_SQUOTE_CACHE: Dict[str, tuple] = {}  # sid -> (ts, dict)
_SQUOTE_TTL = 2.0

# ====== 权威金额稳定器（按日） ======

_AUTH_MAX: Dict[Tuple[str, str], float] = {}  # (code_std, day) -> max

def _stable_authoritative(code_std: str, cand: Optional[float]) -> Optional[float]:
    """cand 明显小于历史最大值：回退；异常大幅回落（>15%）直接丢弃；否则更新（按日隔离）。"""
    day = today_str_cst()
    if cand is None or not (isinstance(cand, (int, float)) and math.isfinite(cand) and cand > 0):
        return None
    prev = _AUTH_MAX.get((code_std, day))
    drop_threshold = 0.85
    if prev is not None and cand < prev * drop_threshold:
        return prev
    if prev is not None and cand < prev * 0.98:
        return prev
    _AUTH_MAX[(code_std, day)] = cand
    return cand

# ====== in-memory 权威金额缓存（按日 + TTL） ======

_AUTH_CACHE_TODAY: Dict[Tuple[str, str], Tuple[float, Optional[float]]] = {}  # (code, day)->(ts, val)
_AUTH_SOURCE_TODAY: Dict[Tuple[str, str], Optional[str]] = {}  # (code, day)->source
_AUTH_TTL_OK   = 60.0   # 有效值缓存 60s
_AUTH_TTL_NONE = 10.0   # None 仅缓存 10s


def get_auth_today_cached(code_std: str) -> Optional[float]:
    day = today_str_cst()
    key = (code_std, day)
    now = time.time()
    if key in _AUTH_CACHE_TODAY:
        ts, v = _AUTH_CACHE_TODAY[key]
        ttl = _AUTH_TTL_NONE if (v is None) else _AUTH_TTL_OK
        if now - ts < ttl:
            return v

    v = None
    src = None
    try:
        v, src = fetch_index_today_amount_authoritative_detail(code_std)
    finally:
        _AUTH_CACHE_TODAY[key] = (now, v)
        _AUTH_SOURCE_TODAY[key] = src
    return v


def get_auth_today_cached_source(code_std: str) -> Optional[str]:
    # 返回 get_auth_today_cached(code_std) 对应的真实来源。
    day = today_str_cst()
    key = (code_std, day)
    _ = get_auth_today_cached(code_std)
    return _AUTH_SOURCE_TODAY.get(key)

# ====== s_ 涨跌幅解析（优先常位，再回退扫描） ======

def _extract_pct_from_s_parts(parts: List[str]) -> Optional[float]:
    def _to_float(x):
        try:
            v = float(str(x).replace("%",""))
            return v if math.isfinite(v) else None
        except Exception:
            return None
    # 常见位优先（不同标的可能略有偏移）
    for idx in (32, 31, 30, 29):
        if len(parts) > idx and isinstance(parts[idx], str) and parts[idx].endswith("%"):
            v = _to_float(parts[idx])
            if v is not None and -100 < v < 100:
                return v
    # 回退扫描
    for p in parts:
        if isinstance(p, str) and p.endswith("%"):
            v = _to_float(p)
            if v is not None and -100 < v < 100:
                return v
    return None

# ====== s_ 简要行情（点位/涨跌幅/金额） ======

def fetch_index_simple_quote(code_std: str) -> Optional[dict]:
    sid = f"s_{code_std}"
    now_ts = time.time()
    if sid in _SQUOTE_CACHE:
        ts, val = _SQUOTE_CACHE[sid]
        if now_ts - ts < _SQUOTE_TTL:
            return val

    # Wind 主源：成功则直接返回；失败自动继续走原腾讯 s_ 逻辑
    wind_sq = fetch_wind_index_snapshot(code_std)
    if wind_sq is not None:
        _SQUOTE_CACHE[sid] = (now_ts, wind_sq)
        return wind_sq

    url = Q_SIMPLE_QUOTE + sid

    def _try_once():
        r = requests.get(url, timeout=TIMEOUT_SEC, headers={"User-Agent": random.choice(UA_POOL)})
        if r.status_code != 200 or not r.text:
            return None
        txt = r.text.strip()
        if not txt.startswith(f"v_{sid}="): return None
        s = txt.split("=", 1)[1].strip().strip('";')
        parts = s.split("~")

        def _to_float(x):
            try:
                v = float(str(x).replace("%",""))
                return v if math.isfinite(v) else None
            except Exception:
                return None

        # 点位 last
        last = None
        for idx in (3, 1):
            if len(parts) > idx:
                v = _to_float(parts[idx])
                if v is not None and 300 <= v <= 20000:
                    last = float(v); break
        if last is None:
            for p in parts:
                v = _to_float(p)
                if v is not None and 300 <= v <= 20000:
                    last = float(v); break

        # 涨跌幅 pct（使用加强版提取）
        pct = _extract_pct_from_s_parts(parts)

        # 成交额（万元→元）
        RANGE_MIN_YUAN, RANGE_MAX_YUAN = 5e9, 2e12  # 50亿 ~ 2万亿
        amt_yuan = None
        idx7_cand_logged = None
        if len(parts) > 7:
            v7 = _to_float(parts[7])
            if v7 is not None:
                cand7 = v7 * 1e4
                idx7_cand_logged = cand7
                if RANGE_MIN_YUAN <= cand7 <= RANGE_MAX_YUAN:
                    amt_yuan = cand7
                else:
                    print(f"[squote] {code_std} 第8字段金额异常 cand={cand7:.3g}（将尝试回退字段）")
        if amt_yuan is None:
            for idx in range(6, min(len(parts), 13)):
                v = _to_float(parts[idx])
                if v is None:
                    continue
                cand = v * 1e4
                if RANGE_MIN_YUAN <= cand <= RANGE_MAX_YUAN:
                    print(f"[squote] {code_std} 使用回退金额字段 idx={idx}（fallback from idx7，idx7_cand={idx7_cand_logged if idx7_cand_logged is not None else 'NA'}） cand={cand:.3g}")
                    amt_yuan = cand
                    break

        amt_yuan = _stable_authoritative(code_std, amt_yuan)
        return {
            "last": last,
            "pct": pct,
            "amt_yuan": amt_yuan,
            "source": "tencent_s",
            "quote_source": "tencent_s",
            "amount_source": "tencent_s" if amt_yuan is not None else None,
        }

    # 轻量重试 1 次，降低瞬断
    for attempt in range(2):
        try:
            out = _try_once()
            if out is not None:
                _SQUOTE_CACHE[sid] = (now_ts, out)
                return out
        except Exception:
            pass
        time.sleep(0.05)
    return None

# ====== QQ 分时（仅用于点位补尾） ======

def http_get(url, params=None) -> Optional[str]:
    headers = {
        "User-Agent": random.choice(UA_POOL),
        "Referer": "https://gu.qq.com/",
        "Accept": "*/*", "Accept-Language": "zh-CN,zh;q=0.9", "Connection": "keep-alive",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT_SEC)
        if r.status_code == 200: return (r.text or "").strip()
        else: print(f"[net] HTTP {r.status_code} @ {url}")
    except Exception as e:
        print(f"[net] 请求失败 @ {url} ：{repr(e)}")
    return None

def parse_mkline_json(txt: str) -> dict:
    if not txt: return {}
    s = txt.strip()
    for prefix in ("v_mkline=", "v_minKline="):
        if s.startswith(prefix): s = s[len(prefix):]; break
    if s.endswith(";"): s = s[:-1]
    try: return json.loads(s)
    except Exception:
        try: return json.loads(txt)
        except Exception: return {}

def extract_m1_array(j: dict, code_key: str):
    if not isinstance(j, dict): return []
    data = (j.get("data") or {}).get(code_key) or {}
    for key in ("m1","m1_ext"):
        arr = data.get(key) or (data.get("data") or {}).get(key)
        if isinstance(arr, list) and arr: return arr
    # DFS
    def _dfs(o):
        if isinstance(o, dict):
            for k,v in o.items():
                if k in ("m1","m1_ext") and isinstance(v, list) and v: return v
                r = _dfs(v)
                if r: return r
        elif isinstance(o, list):
            for it in o:
                r = _dfs(it)
                if r: return r
        return None
    return _dfs(data) or []

def rows_to_df_m1(arr, code_std: str) -> pd.DataFrame:
    rows = []
    for it in arr:
        if isinstance(it, list) and len(it) >= 6:
            if len(it) >= 7: t,o,c,h,l,v,_amt = it[:7]
            else: t,o,c,h,l,v = it[:6]
        elif isinstance(it, str):
            ps = it.replace("\t"," ").split()
            if len(ps) >= 6: t,o,c,h,l,v = ps[:6]
            else: continue
        else:
            continue
        rows.append([t,o,c,h,l,v])
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["time","open","close","high","low","vol"])

    # [PATCH 6] 仅允许两种严格格式：'YYYY-MM-DD HH:MM' 或 'YYYYMMDDHHMM'
    t0 = str(rows[0][0])
    if "-" in t0:
        df["datetime"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M", errors="coerce")
    else:
        df["datetime"] = pd.to_datetime(df["time"], format="%Y%m%d%H%M", errors="coerce")
    df = df.dropna(subset=["datetime"])

    for c in ["open","close","high","low","vol"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    mkt, pure, _ = _parse_code_market(code_std)
    is_index = _is_index(mkt, pure)
    if is_index:
        for c in ["open","close","high","low"]:
            df = df[(df[c] >= 500) & (df[c] <= 20000)]
    else:
        for c in ["open","close","high","low"]:
            df = df[(df[c] >= 0.1) & (df[c] <= 100000)]
    df["code"] = code_std
    df["date"] = df["datetime"].dt.date.astype(str)
    df["hhmm"] = df["datetime"].dt.strftime("%H:%M")
    df = df.drop_duplicates(subset=["code","datetime"]).sort_values("datetime").reset_index(drop=True)
    if not is_index:
        df = _ensure_amount(df)
    else:
        df["_amount_estimated"] = False
    return df


def fetch_today_patch_minutes(code_std: str) -> Tuple[pd.DataFrame, Optional[str]]:
    # 获取今日分钟补尾数据，返回：(df, source)
    wind_df = fetch_wind_today_minutes(code_std)
    if not wind_df.empty:
        wind_df = wind_df.copy()
        wind_df["_patch_source"] = "wind_minutes"
        return wind_df, "wind_minutes"

    param = f"{code_std},m1,,4000"
    txt = None
    for ep in Q_MKLINE_ENDPOINTS:
        txt = http_get(ep, {"param": param, "r": f"{random.random():.6f}"})
        if txt:
            break

    j = parse_mkline_json(txt or "")
    arr = extract_m1_array(j, code_std)
    df = rows_to_df_m1(arr, code_std)
    if df.empty:
        return df, None

    df = df[df["date"] == today_str_cst()].copy()
    if not df.empty:
        df["_patch_source"] = "qq_minutes"
    return df, "qq_minutes"


def fetch_tencent_today_minutes(code_std: str) -> pd.DataFrame:
    # 兼容旧入口：仍然只返回 df。新逻辑请优先使用 fetch_today_patch_minutes()。
    df, _ = fetch_today_patch_minutes(code_std)
    return df

# ====== pytdx 历史分钟 ======
def fetch_minutes_tdx(code: str, freq: str = "1m",
                      start_date: Optional[str] = None,
                      end_date: Optional[str]   = None,
                      verbose: bool = True) -> pd.DataFrame:
    if freq not in CAT_MAP: raise ValueError(f"freq 不支持：{list(CAT_MAP.keys())}")
    cat = CAT_MAP[freq]
    mkt, pure, code_std = _parse_code_market(code)
    is_index = _is_index(mkt, pure)
    dt_start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    dt_end   = datetime.strptime(end_date,   "%Y-%m-%d").date() if end_date   else None

    api = TdxHq_API()
    ip, port = _best_server(api)
    if verbose:
        print(f"[pytdx] 连接 {ip}:{port} …  标的={code_std} 频率={freq} 模式={'index' if is_index else 'security'}")

    frames, start = [], 0
    with api.connect(ip, port):
        page = 0
        while True:
            bars = (api.get_index_bars(cat, mkt, pure, start, PAGE_SIZE)
                    if is_index else
                    api.get_security_bars(cat, mkt, pure, start, PAGE_SIZE))
            page += 1
            if not bars:
                if verbose: print(f"[pytdx] 第 {page} 页无数据，结束。")
                break

            df = pd.DataFrame(bars)
            dt_series = pd.to_datetime(df.get("datetime", pd.Series([], dtype="object")), errors="coerce")
            df = df[~dt_series.isna()].copy()
            if df.empty:
                start += PAGE_SIZE; continue
            df["datetime"] = dt_series[~dt_series.isna()]
            df = df[(df["datetime"].dt.year>=1990) & (df["datetime"].dt.year<=2100)]
            if df.empty:
                start += PAGE_SIZE; continue

            df = _clean_price_rows(df, is_index)
            if df.empty:
                start += PAGE_SIZE; continue

            if dt_start or dt_end:
                d = df["datetime"].dt.date
                mask = pd.Series(True, index=df.index)
                if dt_start: mask &= (d >= dt_start)
                if dt_end:   mask &= (d <= dt_end)
                df = df[mask]

            if not df.empty:
                frames.append(df)
                if verbose:
                    print(f"[pytdx] 第 {page:>2} 页：start={start:<5} 条数={len(df):<4} 区间=({df['datetime'].min()} ~ {df['datetime'].max()})")

            start += PAGE_SIZE
            time.sleep(0.05)

    if not frames:
        if verbose: print("[pytdx] 最终无数据。")
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["datetime"] = pd.to_datetime(out["datetime"])
    out = out.sort_values("datetime").reset_index(drop=True)
    out["code"] = code_std
    out["date"] = out["datetime"].dt.date.astype(str)
    out["hhmm"] = out["datetime"].dt.strftime("%H:%M")
    out = out.drop_duplicates(subset=["code","datetime"]).reset_index(drop=True)
    keep = ["datetime","open","close","high","low","vol","amount","code","date","hhmm"]
    out = out[[c for c in keep if c in out.columns]]

    if "amount" in out.columns:
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    if is_index and "amount" in out.columns:
        out = _normalize_index_amount_units(out, code_std)

    if not is_index:
        out = _ensure_amount(out)
    else:
        out["_amount_estimated"] = False

    if verbose:
        days = sorted(out["date"].unique().tolist())
        print(f"[pytdx] 合计 {len(out)} 行；覆盖交易日={len(days)} 天；样本日={days[-min(12,len(days)):]}")

    return out

# ====== 缓存 ======
def _sanitize_for_cache(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    drop_cols = ["time", "_src", "_has_amt", "_src_rank", "_patch_source"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore").copy()
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    for c in ["code", "date", "hhmm"]:
        if c in df.columns: df[c] = df[c].astype(str)
    for c in ["open","close","high","low","vol","amount"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    if "_amount_estimated" in df.columns:
        s = pd.Series(df["_amount_estimated"], dtype="boolean")
        df["_amount_estimated"] = s.fillna(False).astype(bool)
    subset = [c for c in ["code","datetime"] if c in df.columns]
    if subset:
        df = df.drop_duplicates(subset=subset).sort_values("datetime").reset_index(drop=True)
    return df

def cache_path_for_day(code_std: str, day_str: str) -> str:
    folder = os.path.join(CACHE_DIR, code_std); ensure_dir(folder)
    return os.path.join(folder, f"{day_str.replace('-','')}.parquet")

def _gc_cache_days(code_std: str, keep_days=180):
    folder = os.path.join(CACHE_DIR, code_std)
    if not os.path.isdir(folder): return
    files = sorted([f for f in os.listdir(folder) if f.endswith((".parquet",".csv"))])
    if len(files) <= keep_days: return
    for f in files[:-keep_days]:
        try: os.remove(os.path.join(folder, f))
        except Exception: pass

def _atomic_write_dataframe(df: pd.DataFrame, path: str):
    global _PARQUET_WARNED_ONCE
    tmp = path + f".tmp_{os.getpid()}_{int(time.time()*1000)}"
    try:
        df.to_parquet(tmp, index=False); os.replace(tmp, path)
    except Exception as e:
        try:
            csvp = path.replace(".parquet",".csv")
            df.to_csv(tmp, index=False, encoding="utf-8-sig"); os.replace(tmp, csvp)
            if not _PARQUET_WARNED_ONCE:
                print(f"[cache] Parquet 写入失败，已降级 CSV：{os.path.basename(csvp)}；原因：{repr(e)}")
                _PARQUET_WARNED_ONCE = True
        except Exception:
            try:
                if os.path.exists(tmp): os.remove(tmp)
            except Exception:
                pass

def save_minutes_to_cache(df: pd.DataFrame):
    if df.empty: return
    df = _sanitize_for_cache(df)
    code = df["code"].iloc[0]
    now_ts = time.time()
    for d, chunk in df.groupby("date"):
        p = cache_path_for_day(code, d)
        if os.path.exists(p):
            try:
                if now_ts - os.path.getmtime(p) < CACHE_WRITE_COOLDOWN:
                    continue
            except Exception:
                pass
        _atomic_write_dataframe(chunk, p)
    _gc_cache_days(code)

def load_minutes_from_cache(code_std: str, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
    folder = os.path.join(CACHE_DIR, code_std)
    if not os.path.isdir(folder): return pd.DataFrame()
    frames = []
    for f in sorted(os.listdir(folder)):
        if not (f.endswith(".parquet") or f.endswith(".csv")): continue
        ds = f.split(".")[0]
        if len(ds) != 8: continue
        d = f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}"
        if start_date and d < start_date: continue
        if end_date and d > end_date: continue
        fp = os.path.join(folder, f)
        try:
            df = pd.read_parquet(fp) if f.endswith(".parquet") else pd.read_csv(fp)
            frames.append(df)
        except Exception as e:
            print(f"[cache] 读取失败已跳过：{f}；原因：{repr(e)}")
    if not frames: return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = _sanitize_for_cache(out)
    return out
# ====== 今日权威金额（Wind / 腾讯 s_，带真实来源） ======

def fetch_index_today_amount_authoritative_detail(code_std: str) -> Tuple[Optional[float], Optional[str]]:
    # 今日权威成交额，返回：(amount_yuan, source)
    sq = fetch_index_simple_quote(code_std)
    if sq and isinstance(sq.get("amt_yuan"), (int, float)):
        src = sq.get("amount_source") or sq.get("source") or "unknown_quote"
        return _stable_authoritative(code_std, sq["amt_yuan"]), src

    sid = f"s_{code_std}"
    url = Q_SIMPLE_QUOTE + sid
    try:
        r = requests.get(url, timeout=TIMEOUT_SEC, headers={"User-Agent": random.choice(UA_POOL)})
        if r.status_code != 200 or not r.text:
            return None, None
        txt = r.text.strip()
        if not txt.startswith(f"v_{sid}="):
            return None, None
        s = txt.split("=", 1)[1].strip().strip('";')
        parts = s.split("~")

        def _to_float(x):
            try:
                v = float(str(x).replace("%", ""))
                return v if math.isfinite(v) else None
            except Exception:
                return None

        RANGE_MIN_YUAN, RANGE_MAX_YUAN = 5e9, 2e12
        idx7_cand_logged = None

        if len(parts) >= 8:
            v7 = _to_float(parts[7])
            if v7 is not None:
                cand7 = v7 * 1e4
                idx7_cand_logged = cand7
                if RANGE_MIN_YUAN <= cand7 <= RANGE_MAX_YUAN:
                    return _stable_authoritative(code_std, cand7), "tencent_s_direct"
                else:
                    print(f"[squote] {code_std} 第8字段金额异常 cand={cand7:.3g}（将尝试回退字段）")

        for idx in range(6, min(len(parts), 13)):
            v = _to_float(parts[idx])
            if v is None:
                continue
            cand = v * 1e4
            if RANGE_MIN_YUAN <= cand <= RANGE_MAX_YUAN:
                print(f"[squote] {code_std} 使用回退金额字段 idx={idx}（fallback from idx7，idx7_cand={idx7_cand_logged if idx7_cand_logged is not None else 'NA'}） cand={cand:.3g}")
                return _stable_authoritative(code_std, cand), "tencent_s_direct"

        return None, None
    except Exception:
        return None, None


def fetch_index_today_amount_authoritative(code_std: str) -> Optional[float]:
    # 兼容旧调用：只返回金额。
    v, _ = fetch_index_today_amount_authoritative_detail(code_std)
    return v


# ====== 分析辅助：交易分钟过滤 / 日线聚合 / 均线 / 支撑压力 ======

def filter_cn_trading_minutes(df: pd.DataFrame) -> pd.DataFrame:
    """补丁6：vol/amount 覆盖率低时不强行过滤，只按时段过滤；覆盖率足够再按交易列过滤。"""
    if df.empty: return df
    df = df.drop_duplicates(subset=["code","datetime"]).sort_values("datetime")
    cols = set(df.columns)
    has_any = ("vol" in cols) or ("amount" in cols)

    if has_any:
        mask_has_trade = pd.Series(True, index=df.index)
        if "vol" in cols:    mask_has_trade &= pd.to_numeric(df["vol"], errors="coerce").notna()
        if "amount" in cols: mask_has_trade &= pd.to_numeric(df["amount"], errors="coerce").notna()
        ratio_nonnull = mask_has_trade.mean()
        # 【微调 D】从 0.2 提升到 0.3，更稳
        if ratio_nonnull >= 0.3:
            df = df[mask_has_trade]

    hhmm = df["datetime"].dt.strftime("%H:%M")
    mask = ((hhmm >= "09:30") & (hhmm <= "11:30")) | ((hhmm >= "13:00") & (hhmm <= "15:00"))
    df = df[mask].copy()
    if not df.empty:
        df["hhmm"] = df["datetime"].dt.strftime("%H:%M")
        df["date"] = df["datetime"].dt.date.astype(str)
    return df.reset_index(drop=True)

def minutes_to_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    df = df.sort_values("datetime")
    agg = {}
    if "open"   in df.columns: agg["open"]   = ("open",  "first")
    if "close"  in df.columns: agg["close"]  = ("close", "last")
    if "high"   in df.columns: agg["high"]   = ("high",  "max")
    if "low"    in df.columns: agg["low"]    = ("low",   "min")
    if "vol"    in df.columns: agg["vol"]    = ("vol",   lambda s: s.sum(min_count=1))
    if "amount" in df.columns: agg["amount"] = ("amount",lambda s: s.sum(min_count=1))
    if not agg: return pd.DataFrame()
    out = (
        df.assign(date=pd.to_datetime(df["date"], errors="coerce"))
          .dropna(subset=["date"])
          .groupby("date", as_index=False)
          .agg(**agg)
          .sort_values("date")
          .reset_index(drop=True)
    )
    return out

def moving_averages(daily: pd.DataFrame, windows=(5,10,20)) -> pd.DataFrame:
    if daily.empty: return daily
    out = daily.copy()
    for w in windows:
        if "close" in out.columns:
            out[f"ma{w}"] = out["close"].rolling(w).mean()
    return out

def support_resistance(daily: pd.DataFrame, lookback=20):
    if daily.empty: return None, None
    tail = daily.tail(lookback)
    return (tail["low"].min() if "low" in tail.columns else None), (tail["high"].max() if "high" in tail.columns else None)


# ====== 分时强弱（分钟均额、近1小时/5分钟相对） ======

def _take_current_segment(dft: pd.DataFrame) -> pd.DataFrame:
    if dft.empty: return dft
    hhmm = dft["hhmm"]
    if (hhmm >= "13:00").any(): return dft[hhmm >= "13:00"]
    return dft[hhmm >= "09:30"]

def amt_intraday_stats(df: pd.DataFrame):
    if df.empty or "amount" not in df.columns: return None, None, None
    dft = filter_cn_trading_minutes(df)
    if dft.empty: return None, None, None
    today = dft["date"].max()
    dft_today = dft[dft["date"] == today].copy()
    if dft_today.empty: return None, None, None
    seg = _take_current_segment(dft_today)
    if seg.empty: return None, None, None
    day_avg = seg["amount"].mean()
    if not (isinstance(day_avg,(int,float)) and math.isfinite(day_avg) and day_avg != 0): return None, None, None
    last60 = seg.tail(_bars_for_minutes(FREQ, 60))
    last5  = seg.tail(_bars_for_minutes(FREQ, 5))
    last60_avg = last60["amount"].mean() if not last60.empty else None
    last5_avg  = last5["amount"].mean()  if not last5.empty  else None
    return day_avg, last60_avg, last5_avg


# ====== 同刻累积口径（用于“盘中判定”：今天到当前时刻 vs 历史同刻均额） ======

def intraday_amt_ratio(df_all: pd.DataFrame, window_days=5, today_str_arg: Optional[str]=None):
    """补丁2：真正按 window_days 取同刻历史，样本不足逐级降到 ≥3。"""
    if df_all.empty: return None, None, None, None, None, None
    df = filter_cn_trading_minutes(df_all)
    if df.empty: return None, None, None, None, None, None
    real_today = today_str_cst()
    trade_days = sorted(df["date"].unique())
    latest_trading_day = trade_days[-1]
    use_today = today_str_arg or latest_trading_day

    # 周末/节假日强制回退到最近交易日
    if use_today not in trade_days:
        use_today = latest_trading_day
    is_real_today = (use_today == real_today) and (use_today in trade_days)

    now_hhmm = df[df["date"] == use_today]["hhmm"].max()
    if pd.isna(now_hhmm) or not isinstance(now_hhmm, str):
        return None, None, None, None, use_today, is_real_today
    if not is_real_today:
        now_hhmm = "15:00"

    df_today = df[(df["date"] == use_today) & (df["hhmm"] <= now_hhmm)]
    today_cum_amt = df_today["amount"].sum() if "amount" in df_today.columns else None

    hist = df[(df["date"] != use_today) & (df["hhmm"] <= now_hhmm)]
    by_day = (hist.sort_values("date").groupby("date")["amount"].sum()) if "amount" in hist.columns else pd.Series(dtype=float)

    target = int(window_days)
    if target < 3: target = 3
    # 尝试逐级降级窗口，直到能凑够 >=3 天样本
    while target >= 3 and by_day.tail(target).shape[0] < 3:
        target -= 1
    last_hist = by_day.tail(target)

    if last_hist.shape[0] < 3:
        return None, today_cum_amt, None, now_hhmm, use_today, is_real_today

    hist_avg_same_time = last_hist.mean() if len(last_hist) else None
    ratio = (today_cum_amt / hist_avg_same_time) if (hist_avg_same_time is not None and hist_avg_same_time > 0 and today_cum_amt is not None) else None
    return ratio, today_cum_amt, hist_avg_same_time, now_hhmm, use_today, is_real_today


# ====== 当日口径校准因子 k（仅指数；用于盘中同刻判定） ======

def compute_intraday_scale_k(df_all: pd.DataFrame, code_std: str, now_hhmm: Optional[str]) -> float:
    mkt, pure, _ = _parse_code_market(code_std)
    if not _is_index(mkt, pure): return 1.0
    # 使用缓存后的权威全日
    A_auth = get_auth_today_cached(code_std)
    if (A_auth is None) or (A_auth <= 0): return 1.0
    dft = filter_cn_trading_minutes(df_all)
    if dft.empty: return 1.0
    today = dft["date"].max()
    if now_hhmm is None:
        now_hhmm = dft[dft["date"] == today]["hhmm"].max()
    seg = dft[(dft["date"] == today) & (dft["hhmm"] <= now_hhmm)]
    if seg.empty or "amount" not in seg.columns: return 1.0
    A_tdx = float(seg["amount"].sum())
    if A_tdx <= 0: return 1.0
    k = A_auth / A_tdx
    if not math.isfinite(k) or k <= 0: return 1.0
    k = max(0.01, min(100.0, k))
    if abs(k-1.0) > 0.02:
        src_label = _source_label(get_auth_today_cached_source(code_std)) if get_auth_today_cached(code_std) is not None else "分钟累计"
        print(f"[align] 应用当日口径校准因子 k={k:.4f}（{src_label} 对齐分钟累计）")
    return k


# ====== 形态 & 技术 ======

def candle_signals_last3(daily: pd.DataFrame):
    if daily.empty or len(daily) < 3: return False, False, False
    last3 = daily.tail(3).reset_index(drop=True)
    flags = [False, False, False]
    # 长下影
    for i in range(3):
        o = float(last3.loc[i, "open"]); c = float(last3.loc[i, "close"])
        h = float(last3.loc[i, "high"]); l = float(last3.loc[i, "low"])
        body = abs(c - o); lower = min(o, c) - l
        if body > 0 and lower > 2 * body: flags[0] = True
    # 大阳线
    for i in range(3):
        o = float(last3.loc[i, "open"]); c = float(last3.loc[i, "close"])
        h = float(last3.loc[i, "high"]); l = float(last3.loc[i, "low"])
        if o > 0:
            pct = (c/o - 1) * 100
            if (h - l) > 0 and abs(c-o) > (h-l)/2 and pct > 2: flags[1] = True
    # 三日止跌回升
    if (last3.loc[0,"close"] < last3.loc[0,"open"] and
        last3.loc[1,"close"] < last3.loc[1,"open"] and
        last3.loc[2,"close"] > last3.loc[2,"open"]):
        flags[2] = True
    return tuple(flags)


# ====== 合并当日补尾到 TDX（Wind分钟/QQ分钟均可；金额仍以 TDX/权威为准） ======

def merge_today_tail(base_df: pd.DataFrame, today_df: pd.DataFrame, patch_source: str = "today_patch") -> pd.DataFrame:
    if base_df.empty and today_df.empty:
        return pd.DataFrame()
    if base_df.empty:
        return today_df.assign(_src=patch_source)
    if today_df.empty:
        return base_df.assign(_src="tdx")

    today = today_str_cst()
    base_hist  = base_df[base_df["date"] != today].copy()
    base_today = base_df[base_df["date"] == today].copy().assign(_src="tdx", _src_rank=1)
    patch      = today_df.copy().assign(_src=patch_source, _src_rank=0)

    merged = (
        pd.concat([base_today, patch], ignore_index=True)
          .assign(_has_amt=lambda x: x["amount"].notna().astype(int) if "amount" in x.columns else 0)
          .sort_values(["datetime", "_has_amt", "_src_rank"], ascending=[True, False, True])
          .drop_duplicates(subset=["code","datetime"], keep="first")
          .sort_values("datetime")
          .reset_index(drop=True)
          .drop(columns=["_has_amt", "_src_rank"], errors="ignore")
    )
    out = pd.concat([base_hist.assign(_src="tdx"), merged], ignore_index=True)
    return out.drop_duplicates(subset=["code","datetime"]).sort_values("datetime").reset_index(drop=True)


# ====== 缓存加载主入口（带今日补尾） ======

def minutes_for_code_with_cache(code: str, start: Optional[str], end: Optional[str], verbose=True):
    df_patch_today = pd.DataFrame()  # 防御：后续任何分支都可以安全返回/引用
    code_std = code if code.startswith(("sh","sz")) else _parse_code_market(code)[2]
    meta = {
        "cache_used": False,
        "today_patch_rows": 0,
        "today_patch_source": None,
        "tdx_fetched": False,
    }
    df_cache = load_minutes_from_cache(code_std, start, end)
    if df_cache.empty:
        df_hist = fetch_minutes_tdx(code, freq=FREQ, start_date=start, end_date=end, verbose=verbose)
        save_minutes_to_cache(df_hist)
        meta["tdx_fetched"] = True
    else:
        df_hist = df_cache
        meta["cache_used"] = True
        # 若缓存最新交易日 < 今天/最近交易日，则增量拉取补齐
        try:
            last_cached_day = str(pd.to_datetime(df_hist["date"]).max().date())
        except Exception:
            last_cached_day = None
        need_refresh = (last_cached_day is None) or (last_cached_day < today_str_cst())
        if need_refresh:
            df_inc = fetch_minutes_tdx(code, freq=FREQ,
                                       start_date=last_cached_day,
                                       end_date=today_str_cst(),
                                       verbose=(verbose and code==MAIN_CODE))
            if not df_inc.empty:
                cols = list(set(df_hist.columns) | set(df_inc.columns))
                df_hist = pd.concat([df_hist[cols], df_inc[cols]], ignore_index=True)
                df_hist = df_hist.drop_duplicates(subset=["code","datetime"]).sort_values("datetime").reset_index(drop=True)
                save_minutes_to_cache(df_inc)
                meta["tdx_fetched"] = True

    df_patch_today, today_patch_source = fetch_today_patch_minutes(code_std)
    if not df_patch_today.empty:
        df_hist = merge_today_tail(df_hist, df_patch_today, today_patch_source or "today_patch")
        meta["today_patch_rows"] = len(df_patch_today)
        meta["today_patch_source"] = today_patch_source
    try:
        save_minutes_to_cache(df_hist[df_hist["date"] == today_str_cst()])
    except Exception as e:
        print(f"[cache] 回写今日失败：{repr(e)}")
    return df_hist, (df_patch_today if not df_patch_today.empty else pd.DataFrame()), meta


# ====== 判定（放量企稳标签） ======

def _safe_isfinite(x) -> bool: return isinstance(x, (int, float)) and math.isfinite(x)

def judge_volume_stabilize(ratio_same_time: Optional[float],
                           r5_full_or_same: Optional[float],
                           r20_full_or_same: Optional[float],
                           slope_sign: int) -> Tuple[str, List[str]]:
    """量能判断：<1.05x 基本持平；1.05~1.15x 略强；>=1.15x 或单项>=1.30x 才算明显放量。"""
    vals = [v for v in [ratio_same_time, r5_full_or_same, r20_full_or_same] if isinstance(v,(int,float)) and math.isfinite(v)]
    reasons = []
    if ratio_same_time is not None: reasons.append(f"同刻对比近5日均额倍数≈{ratio_same_time:.2f}x")
    if r5_full_or_same is not None: reasons.append(f"对比5日倍数≈{r5_full_or_same:.2f}x")
    if r20_full_or_same is not None: reasons.append(f"对比20日倍数≈{r20_full_or_same:.2f}x")

    # 【微调 B】按前缀去重
    seen = set(); uniq = []
    for r in reasons:
        key = r.split("：",1)[0]
        if key not in seen:
            uniq.append(r); seen.add(key)
    reasons = uniq

    cnt_ge_115 = sum(1 for v in vals if v >= 1.15)
    any_ge_130 = any(v >= 1.30 for v in vals)
    any_ge_105 = any(v >= 1.05 for v in vals)
    any_ge_095 = any(v >= 0.95 for v in vals)

    strong = (cnt_ge_115 >= 2) or any_ge_130
    mild   = (not strong) and any_ge_105
    flat   = (not strong) and (not mild) and any_ge_095

    if strong and slope_sign < 0:
        if any_ge_130:
            return "✅放量企稳", reasons if reasons else ["量能显著回升，即便短线走弱"]
        return "⚠️明显放量但未企稳", reasons if reasons else ["放量明显，但价格仍走弱"]

    if strong and slope_sign >= 0:
        return "✅放量企稳", reasons if reasons else ["成交额明显高于历史均值，价格止跌/走稳"]

    if mild:
        return "⚠️量能略强但未企稳", reasons if reasons else ["量能略高于历史均值，但力度仍需确认"]

    if flat:
        return "⚠️量能基本持平，尚未企稳", reasons if reasons else ["量能接近历史均值，尚未形成明确放量"]

    return "❌尚未企稳", reasons if reasons else ["量能与价格均未见企稳特征"]


# ====== 展示：点位选择 & 日线 schema 统一 ======

def _prefer_newer_close_for_display(tdx_today: pd.DataFrame, patch_today: pd.DataFrame, daily_prev_close: Optional[float]=None):
    # 返回: (close, hhmm, used_patch, diff_vs_tdx)
    if (tdx_today is None or tdx_today.empty) and (patch_today is None or patch_today.empty):
        return None, None, False, None

    if tdx_today is None or tdx_today.empty:
        row = patch_today.iloc[-1]
        c = row.get("close")
        return (float(c) if pd.notna(c) else None), str(row.get("hhmm")), True, None

    t_last = tdx_today.iloc[-1]
    t_close = float(t_last.get("close", float("nan"))) if pd.notna(t_last.get("close")) else float("nan")
    t_hhmm  = str(t_last.get("hhmm"))

    if patch_today is None or patch_today.empty:
        return (t_close if math.isfinite(t_close) else None), t_hhmm, False, None

    p_last = patch_today.iloc[-1]
    p_close = float(p_last.get("close", float("nan"))) if pd.notna(p_last.get("close")) else float("nan")
    p_hhmm  = str(p_last.get("hhmm"))

    if p_hhmm > t_hhmm and all(math.isfinite(x) for x in [t_close, p_close]):
        diff = (p_close - t_close) / max(1e-9, t_close)
        if abs(diff) > 0.005:
            return p_close, p_hhmm, True, diff

    if p_hhmm > t_hhmm and math.isfinite(p_close):
        diff = (p_close - t_close) / t_close if math.isfinite(t_close) else None
        return p_close, p_hhmm, True, diff

    best_close = t_close if math.isfinite(t_close) else (p_close if math.isfinite(p_close) else None)
    used_patch = p_hhmm > t_hhmm
    diff = (p_close - t_close) / t_close if used_patch and math.isfinite(t_close) and math.isfinite(p_close) else None
    return best_close, max(t_hhmm, p_hhmm), used_patch, diff


def _ensure_daily_schema(d: pd.DataFrame) -> pd.DataFrame:
    # 统一日表 schema，避免 dtype 不一致问题
    cols = ["date","open","high","low","close","vol","amount"]
    d = d.copy()
    for c in cols:
        if c not in d.columns: d[c] = pd.NA
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    return d


# ====== “昨日同刻 / 近3日同刻均额（展示）” 计算工具 ======
# 说明：这些“展示值”稍后会乘以 k_close（对齐权威），仅影响打印展示，不改变判定逻辑。

def get_prev_trading_day(dft: pd.DataFrame, ref_day: str) -> Optional[str]:
    if dft.empty: return None
    days = sorted(dft["date"].unique())
    if ref_day not in days: return days[-1] if days else None
    idx = days.index(ref_day)
    return days[idx-1] if idx-1 >= 0 else None

def same_time_cum_amount(df_all: pd.DataFrame, day_str: str, hhmm: str) -> Optional[float]:
    if df_all.empty or "amount" not in df_all.columns: return None
    dft = filter_cn_trading_minutes(df_all)
    seg = dft[(dft["date"] == day_str) & (dft["hhmm"] <= hhmm)]
    if seg.empty: return None
    s = float(seg["amount"].sum())
    return s if math.isfinite(s) and s > 0 else None

def last_n_same_time_mean(df_all: pd.DataFrame, ref_day: str, hhmm: str, n: int) -> Optional[float]:
    if df_all.empty or "amount" not in df_all.columns: return None
    dft = filter_cn_trading_minutes(df_all)
    hist = dft[(dft["date"] != ref_day) & (dft["hhmm"] <= hhmm)]
    if hist.empty: return None
    days = sorted(hist["date"].unique())
    if not days: return None
    # 取最近 n 个交易日
    pick = days[-n:]
    g = hist[hist["date"].isin(pick)].groupby("date")["amount"].sum().sort_index()
    if g.empty: return None
    mv = float(g.mean())
    return mv if math.isfinite(mv) and mv > 0 else None


# ====== 主流程：快讯 + 分析 + 结论（含“昨日同刻/近3日同刻”展示回补） ======


def _fmt(v):
    # 技术面展示用：有效数值保留两位小数；异常值显示 "-"。
    try:
        return f"{(v if isinstance(v, (int, float)) and math.isfinite(v) else float('nan')):.2f}"
    except Exception:
        return "-"


def _reason_item(label: str, value: Optional[float]) -> Optional[str]:
    # 结论依据格式化：有效倍数 -> “标签≈x.xx”；无效值 -> None。
    if isinstance(value, (int, float)) and math.isfinite(value) and value > 0:
        return f"{label}≈{value:.2f}x"
    return None


def _scale_or_none(x, k):
    # 展示用缩放：数值按 k 放大/缩小；非有效数值原样返回。
    return (float(x) * k) if (isinstance(x, (int, float)) and math.isfinite(x)) else x


def _same_time_means_for_display(df: pd.DataFrame,
                                 ref_day: str,
                                 hhmm: str,
                                 n_days: int) -> Optional[float]:
    # 计算历史同刻累计成交额均值，用于展示口径。
    if df.empty or "amount" not in df.columns:
        return None

    dft = filter_cn_trading_minutes(df)
    if dft.empty:
        return None

    hist = dft[(dft["date"] != ref_day) & (dft["hhmm"] <= hhmm)]
    if hist.empty:
        return None

    by_day = hist.groupby("date")["amount"].sum().sort_index().tail(n_days)
    if len(by_day) < 3:
        return None

    mv = float(by_day.mean())
    return mv if (math.isfinite(mv) and mv > 0) else None


def count_valid_hist_days_same_time(df: pd.DataFrame,
                                    ref_day: str,
                                    hhmm: str,
                                    window_days=5,
                                    min_minutes=8) -> int:
    # 统计同刻口径下可用的历史样本天数。
    if df.empty or "amount" not in df.columns:
        return 0

    dft = filter_cn_trading_minutes(df)
    hist = dft[(dft["date"] != ref_day) & (dft["hhmm"] <= hhmm)]
    if hist.empty:
        return 0

    last_days = sorted(hist["date"].unique())[-(window_days * 2):]
    hist = hist[hist["date"].isin(last_days)]
    g = hist.groupby("date")
    days = [day for day, chunk in g if chunk["amount"].notna().sum() >= min_minutes]
    return len(sorted(days)[-window_days:])


def load_index_data():
    # 加载三大指数分钟数据、今日补尾数据和元信息。
    data_map: Dict[str, pd.DataFrame] = {}
    today_patch_map: Dict[str, pd.DataFrame] = {}
    meta_map: Dict[str, dict] = {}

    for c in INDEX_CODES:
        df_hist, df_patch_today, meta = minutes_for_code_with_cache(
            c,
            HIST_START,
            HIST_END,
            verbose=(c == MAIN_CODE)
        )
        data_map[c] = df_hist
        today_patch_map[c] = df_patch_today
        meta_map[c] = meta

    return data_map, today_patch_map, meta_map


def print_quick_summary(data_map: Dict[str, pd.DataFrame],
                        today_patch_map: Dict[str, pd.DataFrame],
                        meta_map: Dict[str, dict]) -> None:
    # —— 快讯摘要
    print("\n📰 快讯摘要（近实时，基于分钟数据）")
    for c in INDEX_CODES:
        df_all = data_map.get(c, pd.DataFrame())
        if df_all.empty:
            print(f"- {c.upper()}：数据为空"); continue
        dft = filter_cn_trading_minutes(df_all)
        if dft.empty:
            print(f"- {c.upper()}：无交易时段数据"); continue

        days = sorted(dft["date"].unique().tolist())
        latest_day = days[-1]
        tdx_today = dft[dft["date"] == latest_day].copy()
        patch_today = today_patch_map.get(c, pd.DataFrame())

        # 点位：优先 s_ 简要行情；否则 TDX/QQ 合并
        sq = fetch_index_simple_quote(c)
        daily_tmp = minutes_to_daily(df_all)  # 供涨跌幅回退用

        if sq and isinstance(sq.get("last"), (int, float)) and math.isfinite(sq["last"]):
            disp_close = float(sq["last"])
            pct_val = sq.get("pct")
            pct_txt = f"{pct_val:.2f}%" if isinstance(pct_val, (int, float)) and math.isfinite(pct_val) else "--"
            quote_source = sq.get("quote_source") or sq.get("source")
            src_tip = f"（点位：{_source_label(quote_source)}）"

            # 【微调 A】若 s_ 未给涨跌幅，用前收回算填充
            if pct_txt == "--" and daily_tmp is not None and not daily_tmp.empty and len(daily_tmp) >= 2:
                try:
                    prev = float(daily_tmp["close"].iloc[-2])
                    if prev > 0 and isinstance(disp_close, (int, float)) and math.isfinite(disp_close):
                        pct = (disp_close / prev - 1) * 100.0
                        if math.isfinite(pct):
                            pct_txt = f"{pct:.2f}%"
                except Exception:
                    pass
        else:
            # 回退：TDX/QQ 合并
            # prev_close_daily 安全获取
            prev_close_daily = None
            if not daily_tmp.empty and "close" in daily_tmp.columns and len(daily_tmp) >= 2:
                try:
                    v_prev = float(daily_tmp["close"].iloc[-2])
                    if math.isfinite(v_prev) and v_prev > 0:
                        prev_close_daily = v_prev
                except Exception:
                    prev_close_daily = None

            disp_close, disp_hhmm, used_patch, diff = _prefer_newer_close_for_display(tdx_today, patch_today, prev_close_daily)
            if prev_close_daily and isinstance(prev_close_daily, (int,float)) and prev_close_daily > 0 and isinstance(disp_close, (int,float)):
                pct = ((disp_close/prev_close_daily-1)*100.0)
            else:
                pct = float("nan")
            pct_txt = f"{pct:.2f}%" if isinstance(pct,(int,float)) and math.isfinite(pct) else "--"
            if used_patch:
                patch_source = meta_map.get(c, {}).get("today_patch_source") or "today_patch"
                diff_tip = (f"，较TDX差异：{diff*100:.2f}%" if (diff is not None and abs(diff) > 0.002) else "")
                src_tip = f"（点位：{_source_label(patch_source)}更实时{diff_tip}）"
            else:
                src_tip = "（点位：TDX历史分钟）"

        # 当日金额：优先权威（使用缓存），否则分钟累计
        amt_today_auth = get_auth_today_cached(c)
        if amt_today_auth is not None:
            amt_today = amt_today_auth
            src_amt = f"（成交额：{_source_label(get_auth_today_cached_source(c))}）"
        else:
            amt_today = tdx_today["amount"].sum() if "amount" in tdx_today.columns else float("nan")
            src_amt = "（成交额：分钟累计）"

        try:
            print(f"- {c.upper()}：{float(disp_close):.2f} 点（涨跌幅：{pct_txt}），成交额：{readable_billion_from_yuan(amt_today)}{src_tip}{src_amt}")
        except Exception:
            print(f"- {c.upper()}：-- 点（涨跌幅：{pct_txt}），成交额：{readable_billion_from_yuan(amt_today)}{src_tip}{src_amt}")


def print_technical_reference(daily: pd.DataFrame,
                              is_today: bool,
                              now_hhmm: Optional[str],
                              market_close_hhmm: str) -> None:
    # ========= 形态 & 技术 =========
    if not daily.empty:
        ll, bull, rev = candle_signals_last3(daily)
        print("3) 最近3日K线形态")
        print(f"   - 长下影：{'是' if ll else '否'}；大阳线：{'是' if bull else '否'}；止跌回升组合：{'是' if rev else '否'}")
    if not daily.empty and "close" in daily.columns:
        ma = moving_averages(daily, windows=(5,10,20))
        last = ma.tail(1).iloc[0]
        ma5  = last.get("ma5", float("nan"))
        ma10 = last.get("ma10", float("nan"))
        ma20 = last.get("ma20", float("nan"))
        close_last = last.get("close", float("nan"))
        sup, res = support_resistance(daily, lookback=20)
        pos_word = "收盘" if (not (is_today and (now_hhmm < market_close_hhmm))) else "当前价"
        print("\n📐 技术面参考（上证）")
        print(f"   - MA5：{_fmt(ma5)}，MA10：{_fmt(ma10)}，MA20：{_fmt(ma20)}")
        if isinstance(sup,(int,float)) and isinstance(res,(int,float)) and math.isfinite(sup) and math.isfinite(res):
            print(f"   - 近20日支撑/压力：{sup:.2f} / {res:.2f}（仅参考）")
        try:
            if all(isinstance(x,(int,float)) and math.isfinite(x) for x in [ma5, ma10, ma20]):
                if ma5 >= ma10 >= ma20:
                    print("   - 结构：短中期均线多头排列，关注回踩 MA5 的支撑与延续。")
                if isinstance(close_last,(int,float)) and math.isfinite(close_last):
                    if close_last < ma20:
                        print(f"   - 位置：{pos_word}仍在 MA20 下方，反弹以减压为主，留意 MA20 的压力。")
        except Exception:
            pass

def print_intraday_strength(df_main: pd.DataFrame,
                            use_k: bool,
                            k: float,
                            main_amt_source_label: str) -> None:
    # ========= 分时强弱（盘中）
    if use_k:
        mkt_main, pure_main, _ = _parse_code_market(MAIN_CODE)
        if _is_index(mkt_main, pure_main):
            d_avg, l60, l5 = amt_intraday_stats(df_main)
            if isinstance(k, (int, float)) and math.isfinite(k) and k > 0:
                if d_avg is not None: d_avg *= k
                if l60  is not None: l60  *= k
                if l5   is not None: l5   *= k
            if d_avg and d_avg != 0 and l60 is not None and l5 is not None:
                print(f"C) 分时（金额口径 · 已对齐{main_amt_source_label}）")
                try:
                    recent_label = "最近1小时"
                    try:
                        dft_tmp = filter_cn_trading_minutes(df_main)
                        today_tmp = dft_tmp["date"].max() if not dft_tmp.empty else None
                        seg_tmp = _take_current_segment(dft_tmp[dft_tmp["date"] == today_tmp].copy()) if today_tmp is not None else pd.DataFrame()
                        bars60 = _bars_for_minutes(FREQ, 60)
                        if len(seg_tmp) < bars60:
                            unit_min = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "60m": 60}.get(FREQ, 1)
                            recent_minutes = len(seg_tmp) * unit_min
                            recent_label = f"最近{recent_minutes}分钟" if recent_minutes > 0 else "最近可用时段"
                    except Exception:
                        recent_label = "最近可用时段"

                    print(f"   - 当日分钟均额（当前段）：{readable_billion_from_yuan(d_avg)}")
                    print(f"   - {recent_label} / 分钟均额：{(l60/d_avg):.2f}x")
                    print(f"   - 最近5分钟 / 分钟均额：{(l5/d_avg):.2f}x")
                except Exception:
                    pass

def print_amount_analysis(main_amt_source_label: str,
                          amt_full_today,
                          avg5_full,
                          avg20_full,
                          r5_full,
                          r20_full,
                          same_today_amt_disp_show,
                          now_hhmm: Optional[str],
                          avg5_same_disp_show,
                          avg20_same_disp_show,
                          r5_same_disp_show,
                          r20_same_disp_show,
                          yday_same_amt_show,
                          ratio_today_vs_yday_show,
                          mean_3_same_show,
                          ratio_today_vs_3_show) -> None:
    print("\n📊 数据分析（以上证为主 · 全日口径 + 同刻口径，已对齐当前权威成交额口径）")
    print(f"A) 全日口径（{main_amt_source_label} vs 历史全日）")
    print("   1) 当日全日金额：", readable_billion_from_yuan(amt_full_today))
    print("      前5日全日均额：", readable_billion_from_yuan(avg5_full), "；前20日全日均额：", readable_billion_from_yuan(avg20_full))
    print(f"      - 对比前5日倍数：{fmt_ratio(r5_full)}")
    print(f"      - 对比前20日倍数：{fmt_ratio(r20_full)}")

    print(f"B) 同刻口径（同刻累计×k_close 对齐{main_amt_source_label} vs 历史同刻）")
    print("   2) 当前同刻累计额（展示）：", readable_billion_from_yuan(same_today_amt_disp_show), f"（时刻：{now_hhmm or '-'}）")
    print("      近5日同刻均额（展示）：", readable_billion_from_yuan(avg5_same_disp_show), "；近20日同刻均额（展示）：", readable_billion_from_yuan(avg20_same_disp_show))
    print(f"      - 同刻对比5日倍数（展示）：{fmt_ratio(r5_same_disp_show)}")
    print(f"      - 同刻对比20日倍数（展示）：{fmt_ratio(r20_same_disp_show)}")

    # 【微调 C】口径说明
    print(f"※ 说明：盘后同刻口径为“15:00 时刻累计”并以 k_close 对齐{main_amt_source_label}；全日口径为“{main_amt_source_label}”。两者口径不同，数值可不一致。")

    # ====== 【补回打印】昨日同刻 & 近3日同刻（展示）
    print("   3) 昨日同刻累计额（展示）：", readable_billion_from_yuan(yday_same_amt_show))
    print(f"      - 今日 / 昨日同刻倍数：{fmt_ratio(ratio_today_vs_yday_show)}")
    print("   4) 近3日同刻均额（展示）：", readable_billion_from_yuan(mean_3_same_show))
    print(f"      - 今日 / 近3日同刻倍数：{fmt_ratio(ratio_today_vs_3_show)}")
    # ====== /补回打印 ======

def print_conclusion_section(n_hist_days: int,
                             is_close: bool,
                             tag_same: str,
                             reasons_same: List[str],
                             tag_full: str,
                             reasons_full: List[str]) -> None:
    print("\n🧭 结论（同一把尺）")
    print(f"- 样本有效天数（近{WINDOW_DAYS}日，同刻口径）：{n_hist_days} 天")
    if not is_close:
        print(f"- 盘中结论（基于同刻口径）：{tag_same}")
        if reasons_same:
            for i, r in enumerate(reasons_same, 1):
                print(f"  · 盘中依据{i}：{r}")
        print("- 收盘结论（基于全日口径）：⏳待收盘后判断")
        print("  · 当前仍为盘中，当前累计成交额不能直接作为全日成交额判断。")
    else:
        print(f"- 同刻结论（收盘同刻）：{tag_same}")
        if reasons_same:
            for i, r in enumerate(reasons_same, 1):
                print(f"  · 同刻依据{i}：{r}")
        print(f"- 收盘结论（基于全日口径）：{tag_full}")
        if reasons_full:
            for i, r in enumerate(reasons_full, 1):
                print(f"  · 收盘依据{i}：{r}")

def print_debug_same_time_log(meta_map: Dict[str, dict],
                              df_main: pd.DataFrame,
                              same_ref_day: str,
                              prev_day: Optional[str],
                              now_hhmm: Optional[str],
                              main_amt_source_label: str,
                              k_close: float,
                              same_today_amt_disp,
                              yday_same_amt,
                              mean_3_same,
                              same_today_amt_disp_show,
                              yday_same_amt_show,
                              mean_3_same_show) -> None:
    # 调试输出：打印同刻展示值来源与关键中间量。默认 DEBUG_SAME_TIME_LOG=0 不输出。
    if DEBUG_SAME_TIME_LOG:
        meta_main = (meta_map or {}).get(MAIN_CODE, {})
        print("[debug.same] ====== 同刻展示来源 ======")
        print(f"[debug.same] ref_day={same_ref_day}, prev_day={prev_day}, hhmm<={now_hhmm}")
        print(
            f"[debug.same] cache_used={meta_main.get('cache_used')}, "
            f"today_patch_rows={meta_main.get('today_patch_rows')}, "
            f"today_patch_source={_source_label(meta_main.get('today_patch_source'))}, "
            f"tdx_fetched={meta_main.get('tdx_fetched')}"
        )

        print(f"[debug.same] k_close={k_close:.6f}（展示缩放系数，基于{main_amt_source_label} / 同刻分钟累计）")
        print(f"[debug.same] same_today_amt_disp(before_scale)={same_today_amt_disp}")
        print(f"[debug.same] yday_same_amt(before_scale)={yday_same_amt}")
        print(f"[debug.same] mean_3_same(before_scale)={mean_3_same}")
        print(f"[debug.same] same_today_amt_disp_show={same_today_amt_disp_show}")
        print(f"[debug.same] yday_same_amt_show={yday_same_amt_show}")
        print(f"[debug.same] mean_3_same_show={mean_3_same_show}")
        try:
            _last = str(pd.to_datetime(filter_cn_trading_minutes(df_main)["date"]).max().date())
            print(f"[debug.same] latest_trading_day_in_minutes = {_last}")
        except Exception:
            pass
        print("[debug.same] =========================")


def calc_slope_sign(daily: pd.DataFrame) -> int:
    # 计算最近两日 close 的方向：上涨=1，下跌=-1，持平/不可用=0。
    slope = None
    if not daily.empty and len(daily) >= 2 and "close" in daily.columns:
        try:
            slope = float(daily["close"].iloc[-1]) - float(daily["close"].iloc[-2])
        except Exception:
            slope = None
    return 1 if (slope is not None and slope > 0) else (-1 if (slope is not None and slope < 0) else 0)


def build_reason_lists(r5_same_disp_show,
                       r20_same_disp_show,
                       ratio_today_vs_yday_show,
                       ratio_today_vs_3_show,
                       r5_full,
                       r20_full,
                       reasons_same_raw: List[str],
                       reasons_full_raw: List[str]) -> Tuple[List[str], List[str]]:
    # 构造最终展示用的同刻依据/全日依据；若展示倍数不可用，则回退 judge_volume_stabilize 原始依据。
    reasons_same = []
    for item in [
        _reason_item("今日 / 前5日同刻", r5_same_disp_show),
        _reason_item("今日 / 前20日同刻", r20_same_disp_show),
        _reason_item("今日 / 昨日同刻", ratio_today_vs_yday_show),
        _reason_item("今日 / 近3日同刻", ratio_today_vs_3_show),
    ]:
        if item and item not in reasons_same:
            reasons_same.append(item)
    if not reasons_same:
        reasons_same = reasons_same_raw

    reasons_full = []
    for item in [
        _reason_item("今日 / 前5日全日", r5_full),
        _reason_item("今日 / 前20日全日", r20_full),
    ]:
        if item and item not in reasons_full:
            reasons_full.append(item)
    if not reasons_full:
        reasons_full = reasons_full_raw

    return reasons_same, reasons_full


def build_judge_results(ratio,
                        r5_same_disp_raw,
                        r20_same_disp_raw,
                        r5_full,
                        r20_full,
                        slope_sign: int) -> Tuple[str, List[str], str, List[str]]:
    # 生成同刻/全日两套结论标签和原始依据；只封装原有 judge_volume_stabilize 调用。
    tag_same, reasons_same_raw = judge_volume_stabilize(
        ratio,              # 同刻综合倍数
        r5_same_disp_raw,   # 同刻对比5日倍数（辅助）
        r20_same_disp_raw,  # 同刻对比20日倍数（辅助）
        slope_sign
    )
    tag_full, reasons_full_raw = judge_volume_stabilize(
        None,               # 收盘不看同刻综合
        r5_full,
        r20_full,
        slope_sign
    )
    return tag_same, reasons_same_raw, tag_full, reasons_full_raw


def calc_same_time_sample_days(df_main: pd.DataFrame,
                               dft_main: pd.DataFrame,
                               use_day: Optional[str],
                               now_hhmm: Optional[str]) -> int:
    # 计算同刻口径下的历史有效样本天数。
    hh = (now_hhmm or "15:00")
    ref_day_for_hist = use_day if use_day is not None else (
        dft_main["date"].max() if not dft_main.empty else today_str_cst()
    )
    return count_valid_hist_days_same_time(
        df_main,
        ref_day_for_hist,
        hh,
        window_days=WINDOW_DAYS,
        min_minutes=8
    )


def print_northbound_placeholder() -> None:
    # 北向资金：网页端无权威盘中数据，默认关闭展示。
    if SHOW_NORTHBOUND:
        print("\n💴 北向资金：盘中网页端无权威数据，建议盘后查看。")


def calc_is_close(is_today: bool,
                  now_hhmm: Optional[str],
                  market_close_hhmm: str) -> bool:
    # 判断当前是否按收盘口径展示结论。
    return (not is_today) or (now_hhmm is None) or (now_hhmm >= market_close_hhmm)


def calc_use_k(is_today: bool,
               now_hhmm: Optional[str],
               market_close_hhmm: str) -> bool:
    # 判断盘中是否启用当日口径校准因子 k。
    return is_today and (now_hhmm is not None) and (now_hhmm < market_close_hhmm)


def calc_today_basic_state(df_main: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str], bool]:
    # 计算主标交易分钟、最新交易日、是否为今日。
    dft_main = filter_cn_trading_minutes(df_main)
    latest_day = dft_main["date"].max() if not dft_main.empty else None
    is_today = (latest_day == today_str_cst())
    return dft_main, latest_day, is_today


def quick_summary_and_diag():
    print(">>> 启动 A 股快讯 & 诊断（pytdx 历史 + Wind主源/QQ兜底 + 分析模块）")

    data_map, today_patch_map, meta_map = load_index_data()

    print_quick_summary(data_map, today_patch_map, meta_map)

    print_northbound_placeholder()

    # —— 主诊断
    df_main = data_map[MAIN_CODE]
    if df_main.empty:
        print("\n【诊断】主标的数据为空。"); return

    dft_main, latest_day, is_today = calc_today_basic_state(df_main)

    ratio, today_cum_amt, hist_avg_same_time, now_hhmm, use_day, _ = intraday_amt_ratio(df_main, window_days=WINDOW_DAYS)

    # [补丁4] 收盘判定边界：>= "14:59" 认为已收盘
    market_close_hhmm = "14:59"
    use_k = calc_use_k(is_today, now_hhmm, market_close_hhmm)

    if use_k:
        mkt_main, pure_main, code_std_main = _parse_code_market(MAIN_CODE)
        k = compute_intraday_scale_k(df_main, code_std_main, now_hhmm) if _is_index(mkt_main, pure_main) else 1.0
        if k != 1.0 and today_cum_amt is not None:
            today_cum_amt *= k
            if (hist_avg_same_time is not None) and (hist_avg_same_time > 0):
                ratio = today_cum_amt / hist_avg_same_time
    else:
        k = 1.0

    daily = minutes_to_daily(df_main)
    daily = _ensure_daily_schema(daily if not daily.empty else pd.DataFrame(columns=["date","amount"]))

    # —— 盘后强覆盖“今日全日金额”为腾讯权威（使用缓存）
    if (not use_k):
        amt_auth = get_auth_today_cached(MAIN_CODE)
        if amt_auth is not None:
            ref_day = pd.to_datetime(use_day or today_str_cst(), errors="coerce")
            if daily.empty or (daily["date"] == ref_day).sum() == 0:
                row = {c: pd.NA for c in daily.columns}
                row["date"] = ref_day
                row["amount"] = float(amt_auth)
                daily = pd.concat([daily, pd.DataFrame([row])], ignore_index=True)
            daily.loc[daily["date"] == ref_day, "amount"] = float(amt_auth)
            daily = daily.sort_values("date").reset_index(drop=True)

    # ========= 同时展示“全日口径 + 同刻口径” =========
    # 1) 全日口径
    amt_full_today = get_auth_today_cached(MAIN_CODE)
    if (amt_full_today is None) and (not daily.empty) and ("amount" in daily.columns):
        amt_full_today = float(daily.iloc[-1]["amount"])

    # 历史全日均额：排除当前 ref_day，避免“今日和包含自己的均值比较”
    ref_day_full = pd.to_datetime(use_day or latest_day or today_str_cst(), errors="coerce")
    if ("amount" in daily.columns) and (not daily.empty) and (not pd.isna(ref_day_full)):
        daily_hist_full = daily[daily["date"] < ref_day_full].copy()
    else:
        daily_hist_full = pd.DataFrame()

    avg5_full  = (daily_hist_full["amount"].tail(5).mean()  if ("amount" in daily_hist_full.columns and len(daily_hist_full)>=5)  else None)
    avg20_full = (daily_hist_full["amount"].tail(20).mean() if ("amount" in daily_hist_full.columns and len(daily_hist_full)>=20) else None)
    r5_full  = (amt_full_today / avg5_full)   if (amt_full_today is not None and avg5_full  and avg5_full>0)  else None
    r20_full = (amt_full_today / avg20_full)  if (amt_full_today is not None and avg20_full and avg20_full>0) else None

    # 2) 同刻口径（先算“原始用于判定”的，再做展示缩放）
    if now_hhmm is None:
        now_hhmm = "15:00"
    same_ref_day = use_day if use_day is not None else (dft_main["date"].max() if not dft_main.empty else today_str_cst())

    same_today_amt_disp = today_cum_amt
    if same_today_amt_disp is None:
        dft_all = filter_cn_trading_minutes(df_main)
        seg = dft_all[(dft_all["date"] == same_ref_day) & (dft_all["hhmm"] <= now_hhmm)]
        if DEBUG_SAME_TIME_LOG:
            print(f"[debug.same] today_cum_amt 为 None，改用分段累计（ref_day={same_ref_day}, hhmm<={now_hhmm}），seg_rows={len(seg)}")
        same_today_amt_disp = float(seg["amount"].sum()) if ("amount" in seg.columns and not seg.empty) else None

    avg5_same_disp  = _same_time_means_for_display(df_main, same_ref_day, now_hhmm, 5)
    avg20_same_disp = _same_time_means_for_display(df_main, same_ref_day, now_hhmm, 20)
    r5_same_disp_raw  = (same_today_amt_disp / avg5_same_disp)   if (same_today_amt_disp and avg5_same_disp  and avg5_same_disp>0)  else None
    r20_same_disp_raw = (same_today_amt_disp / avg20_same_disp)  if (same_today_amt_disp and avg20_same_disp and avg20_same_disp>0) else None

    # —— 展示缩放：同刻口径一律对齐权威（不改变“判定”）
    k_close = 1.0
    try:
        A_auth_display = get_auth_today_cached(MAIN_CODE)
        if A_auth_display is not None:
            dft_all_disp = filter_cn_trading_minutes(df_main)
            seg_disp = dft_all_disp[(dft_all_disp["date"] == same_ref_day) & (dft_all_disp["hhmm"] <= now_hhmm)]
            A_tdx_same = float(seg_disp["amount"].sum()) if ("amount" in seg_disp.columns and not seg_disp.empty) else None
            if A_tdx_same and A_tdx_same > 0:
                k_close = A_auth_display / A_tdx_same
                if not math.isfinite(k_close) or k_close <= 0:
                    k_close = 1.0
    except Exception:
        k_close = 1.0

    same_today_amt_disp_show = _scale_or_none(same_today_amt_disp, k_close)
    avg5_same_disp_show      = _scale_or_none(avg5_same_disp,      k_close)
    avg20_same_disp_show     = _scale_or_none(avg20_same_disp,     k_close)
    r5_same_disp_show  = (same_today_amt_disp_show / avg5_same_disp_show)   if (same_today_amt_disp_show and avg5_same_disp_show  and avg5_same_disp_show>0)  else None
    r20_same_disp_show = (same_today_amt_disp_show / avg20_same_disp_show)  if (same_today_amt_disp_show and avg20_same_disp_show and avg20_same_disp_show>0) else None

    # ====== 【补回展示】昨日同刻 & 近3日同刻（展示）
    dft_all = filter_cn_trading_minutes(df_main)
    prev_day = get_prev_trading_day(dft_all, same_ref_day)
    yday_same_amt = same_time_cum_amount(df_main, prev_day, now_hhmm) if prev_day else None
    yday_same_amt_show = _scale_or_none(yday_same_amt, k_close)
    ratio_today_vs_yday_show = (same_today_amt_disp_show / yday_same_amt_show) if (same_today_amt_disp_show and yday_same_amt_show and yday_same_amt_show>0) else None

    mean_3_same = last_n_same_time_mean(df_main, same_ref_day, now_hhmm, n=3)
    mean_3_same_show = _scale_or_none(mean_3_same, k_close)
    ratio_today_vs_3_show = (same_today_amt_disp_show / mean_3_same_show) if (same_today_amt_disp_show and mean_3_same_show and mean_3_same_show>0) else None
    # ====== /补回展示 ======

    main_amt_source_label = (
        _source_label(get_auth_today_cached_source(MAIN_CODE))
        if get_auth_today_cached(MAIN_CODE) is not None
        else "分钟累计"
    )

    print_debug_same_time_log(
        meta_map,
        df_main,
        same_ref_day,
        prev_day,
        now_hhmm,
        main_amt_source_label,
        k_close,
        same_today_amt_disp,
        yday_same_amt,
        mean_3_same,
        same_today_amt_disp_show,
        yday_same_amt_show,
        mean_3_same_show
    )

    print_amount_analysis(
        main_amt_source_label,
        amt_full_today,
        avg5_full,
        avg20_full,
        r5_full,
        r20_full,
        same_today_amt_disp_show,
        now_hhmm,
        avg5_same_disp_show,
        avg20_same_disp_show,
        r5_same_disp_show,
        r20_same_disp_show,
        yday_same_amt_show,
        ratio_today_vs_yday_show,
        mean_3_same_show,
        ratio_today_vs_3_show
    )

    print_intraday_strength(df_main, use_k, k, main_amt_source_label)

    print_technical_reference(daily, is_today, now_hhmm, market_close_hhmm)

    slope_sign = calc_slope_sign(daily)

    # 样本有效天数（同刻）
    n_hist_days = calc_same_time_sample_days(df_main, dft_main, use_day, now_hhmm)

    # 盘中用同刻；收盘用全日
    tag_same, reasons_same_raw, tag_full, reasons_full_raw = build_judge_results(
        ratio,
        r5_same_disp_raw,
        r20_same_disp_raw,
        r5_full,
        r20_full,
        slope_sign
    )

    reasons_same, reasons_full = build_reason_lists(
        r5_same_disp_show,
        r20_same_disp_show,
        ratio_today_vs_yday_show,
        ratio_today_vs_3_show,
        r5_full,
        r20_full,
        reasons_same_raw,
        reasons_full_raw
    )

    is_close = calc_is_close(is_today, now_hhmm, market_close_hhmm)

    print_conclusion_section(
        n_hist_days,
        is_close,
        tag_same,
        reasons_same,
        tag_full,
        reasons_full
    )


def main():
    try:
        quick_summary_and_diag()
    except KeyboardInterrupt:
        print("\n用户中断。")
        raise
    except Exception as e:
        print(f"[fatal] 主流程异常：{repr(e)}")
        raise

if __name__ == "__main__":
    main()
