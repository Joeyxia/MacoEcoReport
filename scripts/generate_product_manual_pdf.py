#!/usr/bin/env python3
import ast
import re
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


ROOT = Path(__file__).resolve().parents[1]
DB_FILE = ROOT / "server" / "db.py"
APP_FILE = ROOT / "server" / "app.py"
STOCK_FILE = ROOT / "server" / "stock_service.py"
DAILY_FILE = ROOT / "scripts" / "daily_refresh.py"
OUTPUT_DIR = ROOT / "outputs" / "pdf"
OUTPUT_PATH = OUTPUT_DIR / "nexo_product_rnd_manual_20260316.pdf"


def build_styles():
  pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
  styles = getSampleStyleSheet()
  styles.add(ParagraphStyle(name="CNTitle", fontName="STSong-Light", fontSize=22, leading=28, alignment=TA_CENTER, textColor=colors.HexColor("#17343c"), spaceAfter=10))
  styles.add(ParagraphStyle(name="CNSubTitle", fontName="STSong-Light", fontSize=10.5, leading=15, alignment=TA_CENTER, textColor=colors.HexColor("#587078"), spaceAfter=18))
  styles.add(ParagraphStyle(name="CNH1", fontName="STSong-Light", fontSize=15, leading=21, textColor=colors.HexColor("#123740"), spaceBefore=10, spaceAfter=8))
  styles.add(ParagraphStyle(name="CNH2", fontName="STSong-Light", fontSize=12, leading=18, textColor=colors.HexColor("#123740"), spaceBefore=8, spaceAfter=6))
  styles.add(ParagraphStyle(name="CNBody", fontName="STSong-Light", fontSize=9.5, leading=14, textColor=colors.HexColor("#233e45"), spaceAfter=4))
  styles.add(ParagraphStyle(name="CNSmall", fontName="STSong-Light", fontSize=8.5, leading=12, textColor=colors.HexColor("#587078"), spaceAfter=3))
  return styles


def esc(text):
  return (
    str(text)
    .replace("&", "&amp;")
    .replace("<", "&lt;")
    .replace(">", "&gt;")
    .replace("\n", "<br/>")
  )


def para(text, style):
  return Paragraph(esc(text), style)


def parse_create_tables():
  text = DB_FILE.read_text(encoding="utf-8")
  blocks = re.findall(r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z0-9_]+)\s*\((.*?)\);\n", text, flags=re.S)
  tables = []
  for table_name, body in blocks:
    fields = []
    for raw in body.splitlines():
      line = raw.strip().rstrip(",")
      if not line:
        continue
      upper = line.upper()
      if upper.startswith("FOREIGN KEY") or upper.startswith("PRIMARY KEY") or upper.startswith("UNIQUE ") or upper.startswith("CONSTRAINT "):
        continue
      m = re.match(r"([a-zA-Z0-9_]+)\s+([A-Z]+(?:\s+[A-Z]+)?)", line)
      if not m:
        continue
      fields.append((m.group(1), m.group(2)))
    tables.append((table_name, fields))
  return tables


def parse_routes():
  text = APP_FILE.read_text(encoding="utf-8")
  pattern = re.compile(r'@app\.route\("([^"]+)", methods=\[([^\]]+)\]\)')
  routes = []
  for path, methods in pattern.findall(text):
    ms = [x.strip().strip('"\'') for x in methods.split(",")]
    routes.append((path, "/".join(ms)))
  return routes


def parse_feature_names():
  text = STOCK_FILE.read_text(encoding="utf-8")
  m = re.search(r"FEATURE_NAMES\s*=\s*\[(.*?)\]\n\n", text, flags=re.S)
  if not m:
    return []
  return ast.literal_eval("[" + m.group(1) + "]")


FIELD_HINTS = {
  "id": "主键或唯一标识。",
  "as_of": "数据快照对应的业务日期。",
  "report_date": "日报或结果所属日期。",
  "generated_at": "生成时间。",
  "created_at": "记录创建时间。",
  "updated_at": "记录更新时间。",
  "payload_json": "完整 JSON 载荷，保留原始结构。",
  "row_json": "工作表单行原始 JSON。",
  "summary_json": "摘要型 JSON 结果。",
  "rows_json": "明细结果 JSON。",
  "status": "业务状态字段。",
  "email": "邮箱地址。",
  "source": "来源系统或数据源。",
  "source_file": "导入源文件名。",
  "notes": "备注信息。",
  "ticker": "股票代码。",
  "metric_name": "指标名称。",
  "metric_value": "指标数值。",
  "model_version": "模型版本号。",
  "run_time": "模型运行时间。",
  "signal": "策略信号或预测方向。",
  "account_id": "账户唯一标识。",
  "market_id": "市场唯一标识。",
  "token_id": "盘口 / outcome 的 token 标识。",
  "auto_trading": "自动交易总开关，0 为关闭，1 为开启。",
  "max_slippage_bps": "允许的最大滑点，单位 bps。",
  "api_key": "API key 明文存储字段，仅用于内部服务。",
  "api_key_enc": "加密存储的 API key。",
  "secret_enc": "加密存储的 secret。",
  "passphrase_enc": "加密存储的 passphrase。",
}


TABLE_DESCRIPTIONS = {
  "model_snapshots": "宏观模型整体快照。",
  "sheet_rows": "Excel 工作表行级镜像。",
  "daily_reports": "日报主表，含文本、路径、AI 字段与载荷。",
  "online_checks": "在线数据校验结果。",
  "subscribers": "订阅用户主表。",
  "email_dispatch_logs": "日报批量发送汇总日志。",
  "email_event_logs": "邮箱事件日志，如欢迎邮件标记。",
  "email_delivery_logs": "单邮箱投递结果日志。",
  "monitor_page_events": "页面访问埋点事件。",
  "monitor_token_usage": "Token 使用埋点。",
  "api_credentials": "内部服务 API key 存储。",
  "daily_report_ai_insights": "AI 日报分析结果。",
  "openrouter_fetch_runs": "OpenRouter 抓取运行批次。",
  "openrouter_top_models": "OpenRouter 模型排行。",
  "openrouter_top_apps": "OpenRouter 应用排行。",
  "openrouter_top_providers": "OpenRouter 供应商排行。",
  "openrouter_top_prompts": "OpenRouter prompts 排行。",
  "ticker_profiles": "股票标的信息。",
  "stock_prices": "股票日线价格。",
  "stock_valuations": "月度估值指标。",
  "stock_financials": "季度财务 / 现金流 / 资产负债数据。",
  "uploaded_files": "股票后台上传或自动抓取文件记录。",
  "model_runs": "股票模型训练运行记录。",
  "prediction_results": "按月预测与回测结果。",
  "feature_importance": "特征重要性。",
  "latest_signals": "每个 ticker 的最新信号。",
  "markets": "Polymarket 市场主数据。",
  "outcomes": "市场 outcome / 盘口定义。",
  "orderbook_snapshots": "订单簿快照。",
  "trade_ticks": "逐笔成交。",
  "relation_graph": "市场关系图谱。",
  "arbitrage_opportunities": "套利机会主表。",
  "trading_accounts": "交易账户映射。",
  "polymarket_api_credentials": "Polymarket 账户凭证。",
  "account_balances": "账户余额快照。",
  "account_positions": "账户持仓快照。",
  "risk_limits": "账户级风险限制。",
  "executions": "执行主表。",
  "execution_legs": "执行腿明细。",
  "authorization_logs": "授权与安全日志。",
  "audit_logs": "全局审计日志。",
  "strategy_configs": "策略开关与参数配置。",
}


API_GROUPS = [
  ("基础健康与认证", [
    ("/api/health", "GET", "后端健康检查。"),
    ("/monitor-api/health", "GET", "监控端健康检查。"),
    ("/monitor-api/auth/google/start", "GET", "发起 Google OAuth 登录。"),
    ("/monitor-api/auth/google/callback", "GET", "Google OAuth 回调。"),
    ("/monitor-api/auth/me", "GET", "获取当前监控端登录态。"),
    ("/monitor-api/auth/logout", "POST", "退出监控端登录。"),
  ]),
  ("宏观模型与日报", [
    ("/api/model/current", "GET/POST", "读取或写入当前宏观模型快照。"),
    ("/api/model/summary", "GET", "获取 Dashboard 摘要。"),
    ("/api/model/tables", "GET", "返回工作表列表。"),
    ("/api/model/workbook", "GET", "工作簿数据概览。"),
    ("/api/model/table/<table_name>", "GET", "读取指定工作表内容。"),
    ("/api/reports", "GET/POST", "日报列表与创建。"),
    ("/api/reports/<report_date>", "GET", "日报详情。"),
    ("/api/reports/<report_date>/analysis", "GET/POST", "日报 AI 分析。"),
    ("/api/reports/<report_date>/insight", "GET/POST", "日报洞察摘要。"),
    ("/api/checks", "POST", "写入在线数据校验。"),
    ("/api/checks/latest", "GET", "获取最近一次数据校验。"),
  ]),
  ("订阅与 AI 助手", [
    ("/api/subscribers", "GET/POST", "订阅用户管理。"),
    ("/api/ai/data-query", "POST", "AI 数据问答与后台解释。"),
  ]),
  ("OpenRouter 与监控后台", [
    ("/api/openrouter/rankings", "GET", "读取 OpenRouter 排行数据。"),
    ("/monitor-api/track/page", "POST", "记录页面访问。"),
    ("/monitor-api/track/token", "POST", "记录 token 使用。"),
    ("/monitor-api/ops/overview", "GET", "运维看板概览。"),
    ("/monitor-api/biz/subscribers", "GET", "订阅用户列表。"),
    ("/monitor-api/biz/subscribers/<email>", "DELETE", "删除订阅邮箱。"),
    ("/monitor-api/data/forms", "GET", "数据表单列表。"),
    ("/monitor-api/data/forms/<name>", "GET", "单表明细。"),
  ]),
  ("股票预测模块", [
    ("/api/stocks/health", "GET", "股票模块健康检查。"),
    ("/api/stocks/tickers", "GET", "ticker 列表。"),
    ("/api/stocks/<ticker>/profile", "GET", "ticker 基本信息。"),
    ("/api/stocks/<ticker>/predict/latest", "GET", "最新预测信号。"),
    ("/api/stocks/<ticker>/backtest/summary", "GET", "回测摘要。"),
    ("/api/stocks/<ticker>/backtest/history", "GET", "回测历史序列。"),
    ("/api/stocks/<ticker>/features/latest", "GET", "最新特征值。"),
    ("/monitor-api/stocks/admin/upload-csv", "POST", "上传并识别 CSV。"),
    ("/monitor-api/stocks/admin/import-csv", "POST", "导入 CSV。"),
    ("/monitor-api/stocks/admin/import-and-refresh", "POST", "导入后训练。"),
    ("/monitor-api/stocks/admin/fetch-yahoo", "POST", "自动抓取 Yahoo CSV 并训练。"),
    ("/monitor-api/stocks/admin/refresh/<ticker>", "POST", "单 ticker 重训。"),
    ("/monitor-api/stocks/admin/data-status", "GET", "股票数据状态。"),
    ("/monitor-api/stocks/admin/tickers/<ticker>/status", "GET", "ticker 明细状态。"),
    ("/monitor-api/stocks/admin/upload-history", "GET", "上传历史。"),
    ("/monitor-api/stocks/admin/train-history", "GET", "训练历史。"),
  ]),
  ("Polymarket MVP", [
    ("/api/v1/system/seed-demo", "GET/POST", "初始化 demo 市场与盘口。"),
    ("/api/v1/accounts/polymarket/connect", "POST", "接入 Polymarket 账户。"),
    ("/api/v1/accounts/polymarket/derive-credentials", "POST", "派生账户凭证。"),
    ("/api/v1/accounts/polymarket/status", "GET", "查询账户状态。"),
    ("/api/v1/accounts/polymarket/enable-auto-trading", "POST", "开启自动交易。"),
    ("/api/v1/accounts/polymarket/disable-auto-trading", "POST", "关闭自动交易。"),
    ("/api/v1/accounts/polymarket/emergency-cancel-all", "POST", "紧急撤单。"),
    ("/api/v1/opportunities/scan", "GET/POST", "扫描套利机会。"),
    ("/api/v1/opportunities", "GET", "机会列表。"),
    ("/api/v1/opportunities/<id>", "GET", "机会详情。"),
    ("/api/v1/opportunities/<id>/execute", "POST", "风险评估后执行。"),
    ("/api/v1/strategies/<id>/toggle", "POST", "策略开关。"),
    ("/api/v1/risk/overview", "GET", "风险总览。"),
    ("/api/v1/executions", "GET", "执行记录。"),
    ("/api/v1/replay/summary", "GET", "回放摘要。"),
  ]),
]


def section_title(text, styles, story):
  story.append(para(text, styles["CNH1"]))


def sub_title(text, styles, story):
  story.append(para(text, styles["CNH2"]))


def body_lines(lines, styles, story):
  for line in lines:
    story.append(para(line, styles["CNBody"]))


def add_table(data, widths, story, font_size=8.5):
  tbl = Table(data, colWidths=widths, repeatRows=1)
  tbl.setStyle(TableStyle([
    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eaf6f3")),
    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d3e5e1")),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ("FONTNAME", (0, 0), (-1, -1), "STSong-Light"),
    ("FONTSIZE", (0, 0), (-1, -1), font_size),
    ("LEADING", (0, 0), (-1, -1), font_size + 3),
    ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#17343c")),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fbfefd")]),
    ("LEFTPADDING", (0, 0), (-1, -1), 5),
    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
    ("TOPPADDING", (0, 0), (-1, -1), 4),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
  ]))
  story.append(tbl)
  story.append(Spacer(1, 6))


def meaning_for_field(name):
  if name in FIELD_HINTS:
    return FIELD_HINTS[name]
  if name.endswith("_json"):
    return "JSON 扩展字段，用于保存结构化明细。"
  if name.endswith("_id"):
    return "关联对象 ID。"
  if name.endswith("_at"):
    return "时间戳字段。"
  if name.startswith("max_"):
    return "风险 / 限额参数。"
  if name.startswith("is_") or name.startswith("enable_"):
    return "布尔开关字段。"
  if name in {"title", "name", "label"}:
    return "展示名称。"
  if name in {"best_bid", "best_ask", "mark_price", "avg_fill_price", "avg_cost", "limit_price"}:
    return "价格相关字段。"
  if name in {"qty", "fill_qty", "size", "volume"}:
    return "数量或成交量字段。"
  return "业务字段，详见对应模块逻辑。"


def build_pdf():
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
  styles = build_styles()
  features = parse_feature_names()
  tables = parse_create_tables()
  routes = parse_routes()

  doc = SimpleDocTemplate(
    str(OUTPUT_PATH),
    pagesize=A4,
    rightMargin=16 * mm,
    leftMargin=16 * mm,
    topMargin=14 * mm,
    bottomMargin=14 * mm,
    title="Nexo 产品研发说明书",
    author="Codex",
  )

  story = []
  story.append(para("Nexo 产品研发说明书", styles["CNTitle"]))
  story.append(para("版本：2026-03-16 | 面向产品、研发、测试、运维与后续交接", styles["CNSubTitle"]))

  section_title("1. 产品概览", styles, story)
  body_lines([
    "Nexo 当前是一个多模块、单仓库的产品体系，主站围绕全球宏观风险监控展开，配套每日自动化日报、邮件订阅、AI 解读、运维监控、OpenRouter 排行展示、股票预测与 Polymarket 量化交易 MVP。",
    "系统采取前后端分离形态：静态页面负责展示与交互，Flask 后端负责数据抓取、模型运行、定时任务、数据库落库、权限校验、邮件投递与 API 输出，SQLite 作为当前主数据库。",
    "部署层面使用阿里云 ECS 承载 API 与站点运行，域名包含 nexo.hk、monitor.nexo.hk 与 api.nexo.hk。监控后台采用 Google OAuth + 邮箱白名单保护。",
  ], styles, story)

  section_title("2. 总体系统架构", styles, story)
  body_lines([
    "整体可抽象为五层：展示层、API 聚合层、任务与算法层、数据接入层、存储与日志层。",
    "展示层：主站页面（Dashboard / Daily Report / Indicators / Glossary / Subscribe / AI Assistant / OpenRouter / Stock Prediction / Polymarket）与监控站页面（登录页 / 运维看板 / 业务运营 / 数据生成工具 / 数据表单 / 股票后台）。",
    "API 聚合层：server/app.py 中的 Flask 路由，对前端提供统一的 JSON 接口，并负责会话、鉴权、限流缓存、错误处理与静态资源服务。",
    "任务与算法层：scripts/daily_refresh.py、scripts/send_daily_emails.py、scripts/import_openai_usage.py、scripts/refresh_openrouter_rankings.py、server/stock_service.py、server/polymarket_service.py。",
    "数据接入层：FRED、Yahoo Finance（Playwright / yfinance / chart API / JSON fallback）、OpenAI API、OpenRouter 排行页、Google OAuth、Gmail SMTP、网站埋点数据。",
    "存储与日志层：SQLite 表结构覆盖宏观、邮件、监控、OpenRouter、股票和 Polymarket 六大业务域。",
  ], styles, story)

  sub_title("2.1 模块关系", styles, story)
  add_table(
    [
      ["模块", "输入", "输出", "依赖关系"],
      ["宏观日报", "Excel 模型 + FRED / 在线数据 + OpenAI", "日报、Dashboard 摘要、AI 洞察", "依赖数据库、定时任务、邮件模块"],
      ["订阅邮件", "日报内容 + subscribers 表", "欢迎邮件 / 日报邮件 / 失败通知", "依赖 daily_reports、email_* 表"],
      ["运维监控", "页面访问埋点 + token 埋点 + DB 查询", "Dashboard 图表、运营数据、表单浏览", "依赖 monitor_* 表与所有业务表"],
      ["OpenRouter", "OpenRouter 网页抓取", "排行榜页面与 API", "依赖 openrouter_* 表"],
      ["股票预测", "CSV / Yahoo 数据", "最新信号、回测、特征重要性", "依赖 stock_*、model_runs、latest_signals"],
      ["Polymarket MVP", "市场 / 盘口 / 账户 / 风控参数", "机会扫描、执行演示、审计", "依赖 polymarket 相关数据表"],
    ],
    [24*mm, 48*mm, 48*mm, 48*mm],
    story,
    font_size=8.2,
  )

  section_title("3. 功能模块说明", styles, story)
  sub_title("3.1 主站功能", styles, story)
  body_lines([
    "Dashboard：展示 14 维宏观监控总分、状态、Latest Report Summary、Top Dimension Contributors、Trigger Alerts、Daily Watched Items、Key Indicators Snapshot 与 All 14 Dimensions Detail。",
    "Daily Report：展示日报正文、核心结论、AI 解读、维度明细与指标明细；支持按日期归档访问。",
    "Indicators：展示指标库内容，来源于 Excel 中的 Indicators 表与后端快照。",
    "Glossary：术语表，中英文说明与业务解释。",
    "Subscribe：订阅页面，用户提交邮箱后写入数据库并触发欢迎邮件。",
    "AI Assistant：通过 OpenAI 对站内数据进行问答与解释。",
    "OpenRouter：展示排行数据，包括模型、应用、Provider、Prompts。",
    "Stock Prediction：按 ticker 查询股票预测信号、回测表现、特征解释和投资者提示。",
    "Polymarket：量化交易 MVP 页面，用于账户接入、机会扫描、风险查看与模拟执行。",
  ], styles, story)

  sub_title("3.2 监控站功能", styles, story)
  body_lines([
    "Index：英文登录首页，需先通过 human verification，再使用 Google OAuth 登录。",
    "Dashboard：访问量、Token 使用量、实时分钟图、累计值等运维指标。",
    "Subscribers：订阅邮箱管理、订阅时间、欢迎邮件是否发送、当日日报是否发送、删除操作。",
    "Forms：把后端数据库中的业务表以页面形式呈现，便于数据巡检。",
    "Data Tool：查看在线数据校验与日报生成相关结果。",
    "Stock Data Admin：CSV 手动上传、自动抓取 Yahoo、导入、训练、进度、识别结果、历史记录。",
  ], styles, story)

  section_title("4. 模型与算法说明", styles, story)
  sub_title("4.1 宏观监控模型", styles, story)
  body_lines([
    "宏观系统核心不是机器学习模型，而是基于 Excel 工作簿的 14 维加权评分模型。模型由 Dimensions、Indicators、Scores 等表共同定义，后端每日刷新输入数据后生成总分、状态、维度贡献和指标快照。",
    "该模型适合解释性与运营展示，核心逻辑是规则 / 权重驱动，而不是黑盒预测。",
  ], styles, story)

  sub_title("4.2 AI 分析模型", styles, story)
  body_lines([
    "AI 分析由 OpenAI 主模型驱动，当前默认配置为 gpt-5.4。其用途包括日报简要结论、详细解读、AI 数据问答，以及必要时的兜底内容生成。",
    "系统对 OpenAI 请求增加了排队、最小调用间隔和重试控制，以降低高频触发 429 的概率。",
  ], styles, story)

  sub_title("4.3 股票预测模型", styles, story)
  body_lines([
    "股票预测使用随机森林双模型体系：RandomForestRegressor 预测未来 1 个月收益，RandomForestClassifier 预测上涨概率；模型版本为 rf-walkforward-v1。",
    "训练方式是 walk-forward（月度滚动），并带有 warmup fallback。训练使用的特征包括价格动量、波动率、均线偏离、估值指标和财务增长类指标。",
    "当前特征列表如下：" + "、".join(features),
  ], styles, story)

  sub_title("4.4 Polymarket MVP 算法", styles, story)
  body_lines([
    "Polymarket 当前不是机器学习系统，而是规则扫描 + 执行仿真系统。核心逻辑是从 YES / NO 盘口中识别无套利条件被破坏的位置，再基于订单簿做 VWAP、滑点与 fill risk 评估。",
    "执行前会经过账户状态、auto_trading 开关、max_slippage_bps 等风控约束，再决定是否允许进入模拟执行。",
  ], styles, story)

  story.append(PageBreak())
  section_title("5. API 接口定义", styles, story)
  body_lines([
    f"当前后端在 Flask 中注册了 {len(routes)} 条路由。下面按业务域进行归类说明，作为研发与联调用手册。",
  ], styles, story)
  for group_name, items in API_GROUPS:
    sub_title(group_name, styles, story)
    add_table(
      [["路径", "方法", "说明"]] + items,
      [82*mm, 24*mm, 68*mm],
      story,
      font_size=8.1,
    )

  section_title("6. 数据库设计与字段定义", styles, story)
  body_lines([
    f"当前 SQLite 数据库共定义 {len(tables)} 张主业务表。下面按表输出用途、字段和说明。字段类型直接来源于当前代码实现。",
    "说明规则：若字段为 created_at / updated_at / *_json / *_id 等通用字段，文档使用统一解释；若为关键业务字段，则补充专门说明。",
  ], styles, story)

  for table_name, fields in tables:
    sub_title(f"6.{tables.index((table_name, fields)) + 1} {table_name}", styles, story)
    story.append(para(TABLE_DESCRIPTIONS.get(table_name, "业务数据表。"), styles["CNBody"]))
    rows = [["字段", "类型", "说明"]]
    for field_name, field_type in fields:
      rows.append([field_name, field_type, meaning_for_field(field_name)])
    add_table(rows, [42*mm, 28*mm, 100*mm], story, font_size=7.8)

  story.append(PageBreak())
  section_title("7. 定时任务与运行链路", styles, story)
  body_lines([
    "daily_refresh.py：每日 8 点前后执行在线抓数与数据校验，9 点生成日报、更新数据库、生成 AI 洞察、刷新 reports 与 latest_snapshot。",
    "send_daily_emails.py：在日报生成后读取订阅用户，发送欢迎邮件或日报摘要邮件，并记录投递日志。",
    "import_openai_usage.py：按分钟 / 小时 / 天从 OpenAI usage 导入 token 消耗数据，用于监控站展示。",
    "refresh_openrouter_rankings.py：抓取 OpenRouter 排行页面，写入 openrouter_* 表，供前端 API 读取。",
    "股票模块后台操作：支持手动上传 CSV 或自动执行 Yahoo 抓取后导入训练，训练完成后更新 latest_signals 与输出文件。",
  ], styles, story)

  section_title("8. 权限、认证与安全说明", styles, story)
  body_lines([
    "主站与监控站首页都接入了 human verification 保护，避免简单脚本直接访问入口页。",
    "monitor.nexo.hk 采用 Google OAuth 登录，并限制到固定 Gmail 白名单账户；退出登录后需重新完成验证流程。",
    "Polymarket 设计遵循非托管思路：当前系统不直接托管用户私钥，凭证表保存的是业务层加密字段，真实签名仍需用户钱包或后续 Signing Service 承担。",
    "邮件、OpenAI、FRED 等外部服务凭证统一通过环境变量或 api_credentials 表读取。",
  ], styles, story)

  section_title("9. 对外依赖与第三方服务", styles, story)
  add_table(
    [
      ["服务", "用途", "当前接入方式"],
      ["FRED", "宏观指标抓取", "REST API + API Key"],
      ["OpenAI", "日报解读、AI 助手、token usage", "REST API + API Key / Admin Key"],
      ["OpenRouter", "排行展示", "网页抓取 + 数据库存储"],
      ["Yahoo Finance", "股票历史数据、估值与财务", "Playwright 登录 / chart API / JSON / yfinance fallback"],
      ["Google OAuth", "监控站登录", "OAuth 2.0 Web 回调"],
      ["Gmail SMTP", "欢迎邮件与日报邮件", "App Password 发信"],
      ["阿里云 ECS", "站点与 API 托管", "systemd + 域名 + HTTPS"],
    ],
    [34*mm, 48*mm, 88*mm],
    story,
    font_size=8.2,
  )

  section_title("10. 当前实现边界与后续建议", styles, story)
  body_lines([
    "宏观模型当前以规则 / 权重模型为主，若后续需要提升预测性，可在现有数据库基础上增设时间序列或分类模型实验模块。",
    "股票预测模型已经可用于研究和页面展示，但对于大规模 ticker 批量训练、特征仓库与模型注册，仍可继续工程化。",
    "Polymarket 目前为 MVP，重点是架构、风控与执行链路闭环，后续需要接入真实 market feed、真实签名与订单状态同步，才能进入真实交易灰度阶段。",
    "若项目继续扩大，建议把 SQLite 迁移到更适合并发和分析的数据库，并把定时任务与爬虫任务拆分为独立 worker。",
  ], styles, story)

  story.append(Spacer(1, 8))
  story.append(para(f"文档生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}（Asia/Shanghai）", styles["CNSmall"]))
  story.append(para("生成依据：当前仓库代码实现（server/app.py、server/db.py、server/stock_service.py、scripts/daily_refresh.py 等）。", styles["CNSmall"]))

  doc.build(story)
  print(OUTPUT_PATH)


if __name__ == "__main__":
  build_pdf()
