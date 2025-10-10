# -*- coding: utf-8 -*-
"""
从 SQLite 读取数据 -> 渲染绿色风格速查表（含详情页路由）-> 保存为 operator.html

用法一（直接运行本文件）：
  python wokun1.py

用法二（作为模块被导入后调用）：
  import wokun1
  wokun1.make_operator_html(
      db_path="operator.db",
      table="detail",
      outfile="operator.html"
  )

可选：若你希望“被导入时自动执行一次”，设置环境变量：
  WOKUN1_AUTORUN=1
"""

import sqlite3, json, re, html, sys, os
import warnings
from typing import List, Tuple, Dict, Any

warnings.filterwarnings("ignore")

# ===== 默认配置（可在 make_operator_html(...) 调用时覆盖）=====
DEFAULT_DB_PATH = "operator.db"
DEFAULT_TABLE   = "detail"
DEFAULT_OUTFILE = "operator.html"

__all__ = [
    "make_operator_html",
    "read_rows",
    "build_table_and_map",
    "render_full_html",
    "write_file_safely",
]

# ========== 业务函数封装 ==========

def read_rows(db_path: str, table: str) -> List[Tuple[str, str, str, str]]:
    """从 SQLite 读取四列数据"""
    if not os.path.exists(db_path):
        sys.exit(f"[ERROR] 数据库不存在：{os.path.abspath(db_path)}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        sql = f'''SELECT "算子 / 概念","直觉解释（人话）","常见应用","详情" FROM "{table}"'''
        rows = cur.execute(sql).fetchall()
    finally:
        cur.close(); conn.close()

    if not rows:
        sys.exit("[ERROR] 表中无数据。")
    return rows

def _id_from_op(op: str) -> str:
    """把‘算子/概念’转成稳定的 id"""
    s = str(op).strip().lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    return s

def build_table_and_map(rows: List[Tuple[str, str, str, str]]) -> Tuple[str, str]:
    """
    生成：
      - TABLE_ROWS: <tbody> 里的所有 <tr> 片段
      - DATA_MAP:   详情的 JSON（字符串形式）
    """
    tbody_parts = []
    details_map: Dict[str, Dict[str, Any]] = {}

    for op, intuition, apps, detail_json in rows:
        _id = _id_from_op(str(op))
        op_html = html.escape(str(op))
        exp_html = html.escape(str(intuition))
        apps_str = str(apps or "").strip()
        chips_html = "".join(
            f'<span class="chip">{html.escape(x)}</span>'
            for x in re.split(r"\s+", apps_str) if x
        )
        tbody_parts.append(f'''<tr data-id="{_id}">
  <td><a class="rowlink" href="#/detail/{_id}"><span class="op">{op_html}</span></a></td>
  <td>{exp_html}</td>
  <td><div class="chips">{chips_html}</div></td>
</tr>''')

        # 详情 JSON 兜底
        try:
            obj = json.loads(detail_json)
            if not isinstance(obj, dict): raise ValueError
            obj.setdefault("bt", op)
            obj.setdefault("bq", apps_str.split()[0] if apps_str else "")
            obj.setdefault("xq", str(detail_json))
        except Exception:
            obj = {"bt": op, "bq": apps_str.split()[0] if apps_str else "", "xq": str(detail_json)}
        details_map[_id] = obj

    TABLE_ROWS = "\n".join(tbody_parts)
    DATA_MAP   = json.dumps(details_map, ensure_ascii=False)
    return TABLE_ROWS, DATA_MAP

def render_full_html(TABLE_ROWS: str, DATA_MAP: str) -> str:
    """把占位符安全替换，返回完整 HTML 字符串"""
    content = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>量化常用算子 · 绿色版速查表</title>
<style>
:root{--bg:#f4f8f4;--card:#ffffff;--text:#1c2b1c;--muted:#5d6b5d;--border:#dce6dc;--row:#f8fbf8;--rowHover:#eef8ee;--accent:#3a9d5d;--accent2:#7ac98c;--chipBg:#e7f5ea;--chipText:#2f6d3f;--inputBg:#f2f7f2;--shadow:0 10px 24px rgba(20,60,20,.08)}
@media (prefers-color-scheme: dark){:root{--bg:#0d1a0d;--card:#132313;--text:#eaf2ea;--muted:#9fb29f;--border:#243824;--row:rgba(255,255,255,.03);--rowHover:rgba(80,150,80,.12);--accent:#7ac98c;--accent2:#3a9d5d;--chipBg:rgba(90,160,90,.15);--chipText:#a6f0b6;--inputBg:rgba(255,255,255,.06);--shadow:0 10px 28px rgba(0,0,0,.45)}}
*{box-sizing:border-box}
body{margin:0;color:var(--text);background:linear-gradient(160deg,#f0f9f0 0%,#f9fff9 100%),var(--bg);font:15px/1.65 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",Arial,sans-serif}
.wrap{max-width:1080px;margin:48px auto;padding:0 20px}
.title{margin:0;font-weight:800;line-height:1.1;letter-spacing:.3px;font-size:clamp(28px,4.2vw,40px);background-image:linear-gradient(92deg,var(--accent),var(--accent2));-webkit-background-clip:text;background-clip:text;color:transparent}
.subtitle{margin:8px 0 0;color:var(--muted);font-size:14px}
.card{background:var(--card);border:1px solid var(--border);border-radius:20px;padding:18px;box-shadow:var(--shadow)}
.toolbar{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
.search{flex:1 1 320px;display:flex;align-items:center;gap:10px;background:var(--inputBg);border:1px solid var(--border);padding:10px 12px;border-radius:14px}
.search input{border:0;background:transparent;outline:none;width:100%;color:var(--text);font-size:14px}
.kbd{margin-left:auto;font-size:12px;color:var(--muted);border:1px solid var(--border);padding:2px 6px;border-radius:6px}
.table-wrap{overflow:auto;border:1px solid var(--border);border-radius:14px}
table{width:100%;border-collapse:separate;border-spacing:0;min-width:780px}
thead th{position:sticky;top:0;background:linear-gradient(180deg,rgba(90,160,90,.05),rgba(255,255,255,0));color:var(--muted);font-size:12px;text-align:left;padding:14px 16px;border-bottom:1px solid var(--border);text-transform:uppercase;letter-spacing:.3px;cursor:pointer;user-select:none}
tbody td{padding:14px 16px;border-bottom:1px solid var(--border);vertical-align:top}
tbody tr:nth-child(odd){background:var(--row)}
tbody tr:hover{background:var(--rowHover);transition:background .18s ease}
.op{display:inline-block;font:600 13px ui-monospace,SFMono-Regular,Consolas,monospace;background:rgba(90,160,90,.15);color:var(--accent);padding:6px 10px;border-radius:10px;border:1px solid var(--border)}
.chips{display:flex;flex-wrap:wrap;gap:8px}
.chip{font-size:12px;padding:6px 10px;border-radius:999px;background:var(--chipBg);color:var(--chipText);border:1px solid var(--border);white-space:nowrap}
.foot{display:flex;justify-content:space-between;color:var(--muted);font-size:12px;margin-top:12px;gap:10px}
/* 详情 */
.detail{display:none}
.back{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:12px;border:1px solid var(--border);background:var(--inputBg);color:inherit;text-decoration:none}
.h2{margin:12px 0 6px;font-weight:800;font-size:22px}
.meta{display:flex;gap:8px;flex-wrap:wrap;margin:4px 0 12px}
.block{border:1px dashed var(--border);border-radius:14px;padding:12px 14px;background:var(--row);margin:10px 0;font-size:15px;line-height:1.9;white-space:pre-wrap}
.rowlink{display:block;color:inherit;text-decoration:none}
</style>
</head>
<body>
<div class="wrap">
  <h1 class="title">量化常用算子 · 绿色版速查表</h1>
  <p class="subtitle">柔和绿色主题 · 支持搜索 · 自适应暗黑模式</p>

  <!-- 列表页 -->
  <section id="list" class="card">
    <div class="toolbar">
      <div class="search">
        <input id="q" type="search" placeholder="搜索：log、动量、相关性…" autocomplete="off" />
        <span class="kbd">/</span>
      </div>
    </div>

    <div class="table-wrap">
      <table id="t">
        <thead>
          <tr><th>算子 / 概念</th><th>直觉解释（人话）</th><th>常见应用</th></tr>
        </thead>
        <tbody>
        {TABLE_ROWS}
        </tbody>
      </table>
    </div>

    <div class="foot"><div>🌿 提示：按 <b>/</b> 聚焦搜索。</div></div>
  </section>

  <!-- 详情页 -->
  <section id="detail" class="card detail">
    <a class="back" href="#/">← 返回列表</a>
    <div id="detail-content"></div>
  </section>
</div>

<script>
// ===== 搜索 =====
const q = document.getElementById('q');
const rows = [...document.querySelectorAll('#t tbody tr')];
q.addEventListener('input', ()=>{
  const kw = q.value.trim().toLowerCase();
  rows.forEach(r => r.style.display = r.textContent.toLowerCase().includes(kw)?'':'none');
});
window.addEventListener('keydown', e=>{ if(e.key==='/' && document.activeElement!==q){ e.preventDefault(); q.focus(); }});

// ===== 路由 + 详情 =====
const listView = document.getElementById('list');
const detailView = document.getElementById('detail');
const detailBox = document.getElementById('detail-content');
const esc = s => String(s).replace(/[&<>"]/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]));

// 详情数据（来自 SQLite 的“详情”列 JSON）
const DETAILS = {DATA_MAP};

// ---- 新增：列表滚动位置保存 / 恢复 ----
const SCROLL_KEY = 'listScrollTop';
function saveListScroll(){
  try{
    const top = window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0;
    sessionStorage.setItem(SCROLL_KEY, String(top));
  }catch(_){}
}
function restoreListScroll(){
  try{
    const v = sessionStorage.getItem(SCROLL_KEY);
    const top = v ? parseInt(v, 10) || 0 : 0;
    // 用 requestAnimationFrame 确保列表先完成渲染
    requestAnimationFrame(()=>{ window.scrollTo(0, top); });
  }catch(_){}
}

function renderDetail(id){
  const tr = document.querySelector(`tr[data-id="${CSS.escape(id)}"]`);
  let name = id, cat = '条目';
  if(tr){
    name = tr.querySelector('.op')?.textContent?.trim() || name;
    const chip = tr.querySelector('.chips .chip');
    if(chip) cat = chip.textContent.trim();
  }
  const d = DETAILS[id];
  const text = d ? d.xq : `${name} —— ${tr?tr.children[1].textContent.trim():''}`;
  detailBox.innerHTML = `<div class="h2">${esc(name)}</div>
    <div class="meta"><span class="chip">${esc(cat)}</span></div>
    <div class="block">${esc(text)}</div>`;
}

function router(){
  const m = (location.hash || '#/').match(/^#\/detail\/([^?#]+)$/);
  if(m){
    // 进入详情页前，记录当前列表滚动位置
    saveListScroll();
    const id = decodeURIComponent(m[1]);
    listView.style.display = 'none';
    detailView.style.display = 'block';
    renderDetail(id);
  }else{
    // 返回列表页，显示并恢复滚动
    detailView.style.display = 'none';
    listView.style.display = 'block';
    restoreListScroll();
  }
}
window.addEventListener('hashchange', router);
document.addEventListener('DOMContentLoaded', router);
window.addEventListener('load', router);
router();
</script>
</body>
</html>
"""
    content_safe = content.replace("{TABLE_ROWS}", "__TABLE_ROWS__").replace("{DATA_MAP}", "__DATA_MAP__")
    final_html = content_safe.replace("__TABLE_ROWS__", TABLE_ROWS).replace("__DATA_MAP__", DATA_MAP)
    return final_html

def write_file_safely(outfile: str, html_text: str) -> None:
    """写入文件并做基本校验提示"""
    with open(outfile, "w", encoding="utf-8") as f:
        f.write(html_text)
    size = os.path.getsize(outfile)
    print(f"[OK] 已生成 {outfile}（{size} 字节） -> {os.path.abspath(outfile)}")
    if size < 4096:
        print("⚠️ 文件异常偏小：请确认 content 模板未被省略，且 SQLite 有数据。")

def make_operator_html(db_path: str = DEFAULT_DB_PATH,
                       table: str   = DEFAULT_TABLE,
                       outfile: str = DEFAULT_OUTFILE) -> None:
    """
    入口函数（可被 main 调用，也可被其它模块直接调用）：
      从 SQLite 读数据 -> 组装 HTML -> 写入 outfile
    """
    rows = read_rows(db_path, table)
    table_rows, data_map = build_table_and_map(rows)
    html_text = render_full_html(table_rows, data_map)
    write_file_safely(outfile, html_text)

# ========== CLI 入口 ==========
def _main():
    """命令行入口：支持环境变量覆盖入参"""
    db_path = os.environ.get("WOKUN1_DB", DEFAULT_DB_PATH)
    table   = os.environ.get("WOKUN1_TABLE", DEFAULT_TABLE)
    outfile = os.environ.get("WOKUN1_OUT", DEFAULT_OUTFILE)
    make_operator_html(db_path=db_path, table=table, outfile=outfile)

# 若作为脚本直接运行
if __name__ == "__main__":
    _main()
else:
    # 若你希望“导入即自动执行一次”，设置环境变量 WOKUN1_AUTORUN=1
    if os.environ.get("WOKUN1_AUTORUN", "").strip() == "1":
        try:
            _main()
        except SystemExit:
            # 保持行为一致，但不影响被导入者
            pass
