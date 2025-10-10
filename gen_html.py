# -*- coding: utf-8 -*-
"""
从 SQLite 读取数据 -> 渲染绿色风格速查表（含详情页路由）-> 控制台输出 HTML
用法：
  1) 确保同目录下已有 operator.db，且有 detail 表（四列中文名）
  2) python make_html_from_sqlite.py > index.html
  3) 双击 index.html 即可使用
"""
import sqlite3, json, re, html, sys, os
import warnings
warnings.filterwarnings("ignore")

# ===== 0) 配置 =====
DB_PATH = "operator.db"     # 如需指定绝对路径，直接改成 r"D:\path\operator.db"
TABLE   = "detail"

# ===== 1) 读取 SQLite =====
if not os.path.exists(DB_PATH):
    sys.exit(f"[ERROR] 数据库不存在：{os.path.abspath(DB_PATH)}")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
# 注意列名为中文，这里用双引号引用，避免歧义
sql = f'''SELECT "算子 / 概念","直觉解释（人话）","常见应用","详情" FROM "{TABLE}"'''
rows = cur.execute(sql).fetchall()
cur.close(); conn.close()

if not rows:
    sys.exit("[ERROR] 表中无数据。")

# ===== 2) 工具函数 =====
def id_from_op(op: str) -> str:
    s = op.strip().lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^a-z0-9_]+", "_", s)   # 保持与前面 HTML 版本一致的 id 规则
    return s

# ===== 3) 生成 <tbody> 的行，以及 DETAILS 映射 =====
tbody_parts = []
details_map = {}  # {id: {bt,bq,xq}}

for op, intuition, apps, detail_json in rows:
    _id = id_from_op(str(op))
    op_html = html.escape(str(op))
    exp_html = html.escape(str(intuition))
    apps = str(apps or "").strip()
    chips_html = "".join(
        f'<span class="chip">{html.escape(x)}</span>'
        for x in re.split(r"\s+", apps) if x
    )
    tbody_parts.append(f'''<tr data-id="{_id}">
  <td><a class="rowlink" href="#/detail/{_id}"><span class="op">{op_html}</span></a></td>
  <td>{exp_html}</td>
  <td><div class="chips">{chips_html}</div></td>
</tr>''')

    try:
        obj = json.loads(detail_json)
        # 兜底字段
        if not isinstance(obj, dict): raise ValueError
        obj.setdefault("bt", op)
        obj.setdefault("bq", apps.split()[0] if apps else "")
        obj.setdefault("xq", str(detail_json))
    except Exception:
        obj = {"bt": op, "bq": apps.split()[0] if apps else "", "xq": str(detail_json)}
    details_map[_id] = obj

TABLE_ROWS = "\n".join(tbody_parts)
DATA_MAP   = json.dumps(details_map, ensure_ascii=False)

# ===== 4) HTML 模板（与你喜欢的绿色风格一致）=====
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
// 搜索
const q = document.getElementById('q');
const rows = [...document.querySelectorAll('#t tbody tr')];
q.addEventListener('input', ()=>{
  const kw = q.value.trim().toLowerCase();
  rows.forEach(r => r.style.display = r.textContent.toLowerCase().includes(kw)?'':'none');
});
window.addEventListener('keydown', e=>{ if(e.key==='/' && document.activeElement!==q){ e.preventDefault(); q.focus(); }});

// 路由 + 详情
const listView = document.getElementById('list');
const detailView = document.getElementById('detail');
const detailBox = document.getElementById('detail-content');
const esc = s => String(s).replace(/[&<>"]/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]));

// 详情数据（来自 SQLite 的“详情”列 JSON）
const DETAILS = {DATA_MAP};

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
    const id = decodeURIComponent(m[1]);
    listView.style.display = 'none';
    detailView.style.display = 'block';
    renderDetail(id);
  }else{
    detailView.style.display = 'none';
    listView.style.display = 'block';
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

# ===== 5) 安全替换占位符并输出 =====
content_safe = content.replace("{TABLE_ROWS}", "__TABLE_ROWS__").replace("{DATA_MAP}", "__DATA_MAP__")
final_html = content_safe.replace("__TABLE_ROWS__", TABLE_ROWS).replace("__DATA_MAP__", DATA_MAP)

# 控制台打印最终 HTML
print(final_html)
