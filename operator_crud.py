# -*- coding: utf-8 -*-
"""
作用：对 operator.db 的 detail 表做 增/删/改/查。
表结构与原脚本一致：
  - "算子 / 概念" (PK)
  - "直觉解释（人话）"
  - "常见应用"
  - "详情" (JSON 字符串)
"""

import os
import json
import sqlite3
from typing import Dict, Any, Iterable, List, Optional, Tuple

DB = "operator.db"

DDL = """
CREATE TABLE IF NOT EXISTS detail (
  "算子 / 概念"      TEXT PRIMARY KEY,
  "直觉解释（人话）" TEXT,
  "常见应用"          TEXT,
  "详情"             TEXT
);
"""

def _connect(db_path: str = DB) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(DDL)
    return conn

# =========================
# 基础 CRUD
# =========================

def add_detail(item: Dict[str, Any], *, upsert: bool = False, db_path: str = DB) -> None:
    """
    新增一条记录。
    参数:
      item = {
        "算子 / 概念": str,
        "直觉解释（人话）": str,
        "常见应用": str,
        "详情": dict 或 JSON字符串
      }
      upsert=True 时使用 INSERT OR REPLACE；否则普通 INSERT（若主键已存在将报错）。
    """
    # 处理详情字段：支持 dict 或 str
    detail_val = item.get("详情")
    if isinstance(detail_val, (dict, list)):
        detail_json = json.dumps(detail_val, ensure_ascii=False)
    elif isinstance(detail_val, str):
        detail_json = detail_val
    else:
        detail_json = json.dumps({}, ensure_ascii=False)

    sql = ("INSERT OR REPLACE" if upsert else "INSERT") + """
    INTO detail ("算子 / 概念","直觉解释（人话）","常见应用","详情")
    VALUES (?,?,?,?)
    """
    params = (
        item.get("算子 / 概念"),
        item.get("直觉解释（人话）"),
        item.get("常见应用"),
        detail_json,
    )
    with _connect(db_path) as conn:
        conn.execute(sql, params)
        conn.commit()

def add_many(items: Iterable[Dict[str, Any]], *, upsert: bool = False, db_path: str = DB) -> int:
    """
    批量新增；返回插入/替换行数。
    """
    sql = ("INSERT OR REPLACE" if upsert else "INSERT") + """
    INTO detail ("算子 / 概念","直觉解释（人话）","常见应用","详情")
    VALUES (?,?,?,?)
    """
    rows: List[Tuple[Any, Any, Any, Any]] = []
    for it in items:
        dv = it.get("详情")
        if isinstance(dv, (dict, list)):
            dv = json.dumps(dv, ensure_ascii=False)
        elif dv is None:
            dv = json.dumps({}, ensure_ascii=False)
        rows.append((
            it.get("算子 / 概念"),
            it.get("直觉解释（人话）"),
            it.get("常见应用"),
            dv if isinstance(dv, str) else str(dv),
        ))
    with _connect(db_path) as conn:
        cur = conn.executemany(sql, rows)
        conn.commit()
        return cur.rowcount or 0

def update_detail(key: str, fields: Dict[str, Any], *, merge_detail: bool = True, db_path: str = DB) -> int:
    """
    更新一条记录（按主键“算子 / 概念”）。
    参数:
      key: 要更新的主键值
      fields: 允许包含 "直觉解释（人话）" / "常见应用" / "详情"
      merge_detail: 若 True 且传入的“详情”为 dict，则与库中的 JSON 进行浅层合并；False 则直接覆盖。
    返回：受影响行数
    """
    with _connect(db_path) as conn:
        # 如需要合并详情，先读旧值
        if "详情" in fields and merge_detail:
            old = conn.execute('SELECT "详情" FROM detail WHERE "算子 / 概念"=?', (key,)).fetchone()
            base = {}
            if old and old["详情"]:
                try:
                    base = json.loads(old["详情"])
                except Exception:
                    base = {}
            incoming = fields["详情"]
            if isinstance(incoming, str):
                try:
                    incoming = json.loads(incoming)
                except Exception:
                    incoming = {"_raw": incoming}
            if isinstance(incoming, dict):
                base.update(incoming)
                fields["详情"] = json.dumps(base, ensure_ascii=False)
            else:
                fields["详情"] = json.dumps(incoming, ensure_ascii=False)
        elif "详情" in fields:
            # 直接覆盖
            dv = fields["详情"]
            if isinstance(dv, (dict, list)):
                fields["详情"] = json.dumps(dv, ensure_ascii=False)

        sets = []
        params: List[Any] = []
        for col in ("直觉解释（人话）", "常见应用", "详情"):
            if col in fields:
                sets.append(f'"{col}"=?')
                params.append(fields[col])
        if not sets:
            return 0  # 无字段可更新

        params.append(key)
        sql = f'UPDATE detail SET {", ".join(sets)} WHERE "算子 / 概念"=?'
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount or 0

def delete_detail(key: str, *, db_path: str = DB) -> int:
    """
    删除一条记录（按主键“算子 / 概念”），返回删除行数。
    """
    with _connect(db_path) as conn:
        cur = conn.execute('DELETE FROM detail WHERE "算子 / 概念"=?', (key,))
        conn.commit()
        return cur.rowcount or 0

# =========================
# 查询/搜索
# =========================

def get_detail(key: str, *, db_path: str = DB) -> Optional[Dict[str, Any]]:
    """
    获取单条；返回 dict（其中“详情”会反序列化为 dict），找不到返回 None。
    """
    with _connect(db_path) as conn:
        row = conn.execute('SELECT * FROM detail WHERE "算子 / 概念"=?', (key,)).fetchone()
        if not row:
            return None
        d = dict(row)
        try:
            d["详情"] = json.loads(d.get("详情") or "{}")
        except Exception:
            pass
        return d

def list_details(limit: int = 50, offset: int = 0, *, db_path: str = DB) -> List[Dict[str, Any]]:
    """
    分页列出；“详情”自动转为 dict。
    """
    with _connect(db_path) as conn:
        cur = conn.execute(
            'SELECT * FROM detail ORDER BY "算子 / 概念" LIMIT ? OFFSET ?',
            (limit, offset)
        )
        out: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            d = dict(row)
            try:
                d["详情"] = json.loads(d.get("详情") or "{}")
            except Exception:
                pass
            out.append(d)
        return out

def search_details(keyword: str, limit: int = 50, *, db_path: str = DB) -> List[Dict[str, Any]]:
    """
    模糊搜索：在 主键、直觉解释、常见应用、详情(JSON文本) 上 LIKE。
    """
    like = f"%{keyword}%"
    sql = """
    SELECT * FROM detail
    WHERE "算子 / 概念" LIKE ?
       OR "直觉解释（人话）" LIKE ?
       OR "常见应用" LIKE ?
       OR "详情" LIKE ?
    ORDER BY "算子 / 概念"
    LIMIT ?
    """
    with _connect(db_path) as conn:
        rows = conn.execute(sql, (like, like, like, like, limit)).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            try:
                d["详情"] = json.loads(d.get("详情") or "{}")
            except Exception:
                pass
            out.append(d)
        return out

# =========================
# 示例用法（直接运行此文件查看）
# =========================

def _demo():
    print(f"数据库：{os.path.abspath(DB)}")

    # 1) 新增（若存在则失败）：
    try:
        add_detail({
            "算子 / 概念": "ema(x, n)",
            "直觉解释（人话）": "“指数加权的均线”。越近的权重越大。",
            "常见应用": "动量 平滑",
            "详情": {"bt": "ema(x, n)", "bq": "动量", "xq": "指数加权平均，响应更快。"},
        }, upsert=False)
        print("✅ 新增成功（ema）")
    except sqlite3.IntegrityError:
        print("⚠️ 已存在（ema），跳过普通 INSERT")

    # 2) 新增（有则覆盖/更新）
    add_detail({
        "算子 / 概念": "ema(x, n)",
        "直觉解释（人话）": "“指数加权均线”。近数据更重要。",
        "常见应用": "动量/跟随",
        "详情": {"note": "upsert 示例"},
    }, upsert=True)
    print("✅ UPSERT 成功（ema）")

    # 3) 更新（合并详情）
    rc = update_detail("ema(x, n)", {
        "常见应用": "动量 跟随 去噪",
        "详情": {"更多": "可以与zscore结合做入场触发"}
    }, merge_detail=True)
    print(f"✅ 更新成功，受影响行数：{rc}")

    # 4) 查询单条
    one = get_detail("ema(x, n)")
    print("🔎 查询单条：", one)

    # 5) 模糊搜索
    hits = search_details("动量", limit=5)
    print(f"🔎 模糊搜索（动量）命中 {len(hits)} 条，示例第一条：", hits[0] if hits else None)

    # 6) 分页列出
    page = list_details(limit=3, offset=0)
    print("📄 列表(3条)：", page)

    # 7) 删除
    rc = delete_detail("ema(x, n)")
    print(f"🗑️ 删除 ema(x, n)：{rc} 行")

if __name__ == "__main__":
    # _demo()
    cxtj = "mean(x, n)"
    # one = get_detail(cxtj)
    # print("🔎 查询单条：", one)

    new_xq = """mean(x, n) → n 日均值，常作均线。
在 BRAIN 里常用写法是 ts_mean(x, n)。如果你看到文档写 mean(x, n)，通常就是 ts_mean 的等价/旧名；推荐用 ts_mean，更直观。
过去 n 天的简单滑动平均（SMA），也就是把同一只股票在最近 n 个交易日的 x 做算术平均，返回“今天”的平均值。
行为与细节

窗口：包含“今天”在内的最近 n 天（滚动更新）。
NaN 规则：窗口内数据不足或全部无效通常返回 NaN；有少量 NaN 时平台一般按有效样本求均值（极端全 NaN → NaN）。
不改变量纲：输入什么单位，输出仍是什么单位（价→价，收益→收益）。

单调不保持：mean 会平滑/滞后，不能作为严格的排序保序函数（不同于 rank）。
直白例子

近 5 天的收盘价：
[10, 12, 8, 15, 9]（今天=9）
ts_mean(close, 5) = (10+12+8+15+9)/5 = 10.8

什么时候用（典型场景）

去噪平滑：
ts_mean(ret_1d, 5)        // 短期收益的平滑版
ts_mean(volume, 20)       // 20日均量
直白解释：
股票每天都涨跌不定，单日数据很乱。
用均值就像“滤镜”，把抖动拉平，看整体趋势。
👉 就像看天气：一天冷一天热没意义，算一周平均气温才知道是不是“冷了”。


相对位置/偏离度：
x - ts_mean(x, n)         // 去均值后的“偏离”
x / ts_mean(x, n) - 1     // 相对均值的百分偏离
sign(x - ts_mean(x, n))   // 在均值上方(1)/下方(-1)
直白解释：
不是看股价绝对多少，而是看“现在比过去平均高还是低”。
如果今天收盘价比20日均价高，说明“强”；低，说明“弱”。
👉 就像体检：不是单看体重，而是和过去几年的平均比，是胖了还是瘦了。

趋势/动量判别（均线交叉）：
sign(ts_mean(close, 10) - ts_mean(close, 60))  // 金叉/死叉方向
直白解释：
短期均线（10日）比长期均线（60日）高 → 上升趋势（多头）。
短期均线在长期均线下 → 下降趋势（空头）。
👉 就像看马路上的车流：短期车速快过长期开的均速 → 趋势向上。

与分组结合（行业中性）：
group_rank(ts_mean(ret_1d, 20), industry)
直白解释：
不在全市场一起比，而是在行业内部比过去20天的平均收益。
这样避免因子只挑某个热门行业（比如全市场低 PE 时，银行股总是排前）。
👉 就像运动会：不是全校混排，而是先在每个班里比，看谁是“班里最强”。


常见套路（可直接抄）
// 1) 20日趋势强度（在均值上方多少）
alpha = (close / ts_mean(close, 20)) - 1
// 2) 短期反转（当日收益相对近20日均值为负则做多）
alpha = -rank(ret_1d - ts_mean(ret_1d, 20))
// 3) 行业内动量（20日平滑收益，行业中性）
alpha = group_rank(ts_mean(ret_1d, 20), industry)
// 4) 均线交叉方向信号
alpha = sign(ts_mean(close, 10) - ts_mean(close, 60))

参数怎么选？
5/10 天：短期（更敏感，噪声大）。
20/60 天：中期（常用动量/均线）。
120 天：长期（大周期趋势）。
经验：短期信号配合去极值/行业中性；长期信号更稳但反应慢

"""

    # 2) 组合更新字段：仅提供 {"详情": {"xq": 新文本}}，其他键不传就不会改
    rc = update_detail(
        key=cxtj,
        fields={"详情": {"xq": new_xq}},
        merge_detail=True  # 关键点：与原 JSON 做浅合并，只覆盖 xq
    )
    print("受影响行数：", rc)
    #
    # # 3) 验证一下
    # after = get_detail(cxtj)
    # print("更新后详情.xq：\n", after["详情"].get("xq"))

    # rc = delete_detail(cxtj)
    #
    import wokun1
    wokun1.make_operator_html()


