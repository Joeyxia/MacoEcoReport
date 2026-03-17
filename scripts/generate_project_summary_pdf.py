#!/usr/bin/env python3
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "outputs" / "pdf"
OUTPUT_PATH = OUTPUT_DIR / "nexo_project_summary_20260316.pdf"


def build_styles():
  pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
  styles = getSampleStyleSheet()
  styles.add(
    ParagraphStyle(
      name="CNTitle",
      fontName="STSong-Light",
      fontSize=22,
      leading=30,
      textColor=colors.HexColor("#16333b"),
      alignment=TA_CENTER,
      spaceAfter=10,
    )
  )
  styles.add(
    ParagraphStyle(
      name="CNSubTitle",
      fontName="STSong-Light",
      fontSize=11,
      leading=16,
      textColor=colors.HexColor("#5b7278"),
      alignment=TA_CENTER,
      spaceAfter=18,
    )
  )
  styles.add(
    ParagraphStyle(
      name="CNHeading",
      fontName="STSong-Light",
      fontSize=14,
      leading=20,
      textColor=colors.HexColor("#12353d"),
      spaceBefore=10,
      spaceAfter=8,
    )
  )
  styles.add(
    ParagraphStyle(
      name="CNBody",
      fontName="STSong-Light",
      fontSize=10.5,
      leading=16,
      textColor=colors.HexColor("#233e45"),
      spaceAfter=5,
    )
  )
  styles.add(
    ParagraphStyle(
      name="CNSmall",
      fontName="STSong-Light",
      fontSize=9,
      leading=13,
      textColor=colors.HexColor("#5b7278"),
      spaceAfter=4,
    )
  )
  return styles


def p(text, style):
  safe = (
    str(text)
    .replace("&", "&amp;")
    .replace("<", "&lt;")
    .replace(">", "&gt;")
    .replace("\n", "<br/>")
  )
  return Paragraph(safe, style)


def section(title, paragraphs, styles, story):
  story.append(p(title, styles["CNHeading"]))
  for item in paragraphs:
    story.append(p(item, styles["CNBody"]))
  story.append(Spacer(1, 4))


def build_pdf():
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
  styles = build_styles()
  doc = SimpleDocTemplate(
    str(OUTPUT_PATH),
    pagesize=A4,
    rightMargin=18 * mm,
    leftMargin=18 * mm,
    topMargin=16 * mm,
    bottomMargin=16 * mm,
    title="Nexo 项目工作总结",
    author="Codex",
  )

  story = []
  story.append(p("Nexo 项目阶段性工作总结", styles["CNTitle"]))
  story.append(
    p(
      "覆盖时间：本轮合作至 2026-03-16 | 输出对象：项目负责人 / 产品 / 技术 / 运维",
      styles["CNSubTitle"],
    )
  )

  summary_table = Table(
    [
      ["主域名", "https://nexo.hk"],
      ["监控域名", "https://monitor.nexo.hk"],
      ["后端域名", "https://api.nexo.hk"],
      ["核心后端", "Flask + SQLite + 定时任务 + 邮件 + OpenAI / Yahoo / OpenRouter 数据接入"],
      ["当前覆盖模块", "宏观监控、日报生成、订阅邮件、运维监控、股票预测、Polymarket MVP"],
    ],
    colWidths=[34 * mm, 132 * mm],
  )
  summary_table.setStyle(
    TableStyle(
      [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eaf6f3")),
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d3e5e1")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTNAME", (0, 0), (-1, -1), "STSong-Light"),
        ("FONTSIZE", (0, 0), (-1, -1), 9.5),
        ("LEADING", (0, 0), (-1, -1), 14),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#16333b")),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#fbfefd")]),
      ]
    )
  )
  story.append(summary_table)
  story.append(Spacer(1, 10))

  section(
    "1. 项目总体成果",
    [
      "已将原始 Excel 宏观监控模型扩展为一套前后端分离、可部署、可运营、可每日自动更新的线上系统。前端负责展示，后端负责抓数、计算、写库、生成日报、调用 AI 分析、发送邮件和对外提供 API。",
      "站点已迁移到阿里云并绑定自有域名 nexo.hk，同时配套上线 monitor.nexo.hk 运维监控平台，实现业务数据、访问量、Token 使用量、订阅用户、后台工具与数据表单的统一管理。",
      "在原宏观系统基础上，进一步新增股票预测子系统，以及 Polymarket 量化交易系统 MVP，为后续扩展到多资产、多策略研究和自动执行打下基础。",
    ],
    styles,
    story,
  )

  section(
    "2. 宏观监控主站建设",
    [
      "完成主站首页、Dashboard、Daily Report、Indicators、Glossary、Subscribe、AI Assistant、OpenRouter 等页面建设，并支持中英文切换。",
      "根据 Excel 中 Dimensions / Indicators / Scores 等工作表，把 14 个维度、指标分数、贡献度、摘要、预警和明细全部打通到前端展示逻辑，修复过多轮字段空值、维度遗漏、移动端左右滚动和版式不一致问题。",
      "完成主页极简科技风格落地，启用 nexo.hk 域名访问，统一品牌为 Nexo Macro Intelligence，并持续把视觉样式统一到网站各页面与监控端页面。",
    ],
    styles,
    story,
  )

  section(
    "3. 数据后端与数据库体系",
    [
      "完成后端服务与 SQLite 数据库建设，把浏览器端数据迁移到后端数据库统一管理。数据库已覆盖模型快照、日报、在线校验、邮件日志、订阅用户、访问统计、Token 使用、OpenRouter 排行、股票数据与模型结果等。",
      "日报生成逻辑已改为后端先抓数、校验、计算、生成最终内容并落库，前端只通过 API 展示，这样既提高一致性，也便于定时任务、邮件分发与监控。",
      "后续新增的 Polymarket 模块也已按 PRD 扩展数据库结构，包括 markets、outcomes、orderbook_snapshots、trade_ticks、arbitrage_opportunities、executions、risk_limits、audit_logs 等核心表。",
    ],
    styles,
    story,
  )

  section(
    "4. 日报自动化生产链路",
    [
      "构建了每日 8 点中国时间在线抓取与更新指标、9 点生成当日日报的自动任务链路；生成后自动写入数据库与报告归档，并用于首页 Dashboard 的摘要区块更新。",
      "日报页面已支持展示报告预览、14 维度明细、指标数据、AI 简要结论与详细解读，并区分中英文版本内容，确保中文页显示中文、英文页显示英文。",
      "针对日报总分连续不变、指标未及时更新、AI 解读未落库或页面未展示等问题做过多轮排查与修复，当前架构支持日报生成后再触发 GPT 分析并回写数据库。",
    ],
    styles,
    story,
  )

  section(
    "5. 在线数据抓取与外部数据源接入",
    [
      "已接入并调试过多类外部数据源，包括 FRED、Yahoo Finance、OpenRouter 排行页、OpenAI Usage、网站访问日志等；对无法直接获取或延迟较高的数据源建立了校验日志、失败记录与替代数据源讨论流程。",
      "为 Yahoo Finance 股票数据抓取做了从手动 CSV 上传到 Playwright 自动登录抓取的演进，同时增加 yfinance 与 Yahoo JSON / chart API 等 fallback 逻辑，尽量减少因登录挑战、crumb 丢失、接口受限导致的失败。",
      "为 OpenRouter 页面增加了后端定时抓取与数据库落库机制，使前端页面不依赖浏览器实时抓取即可展示排行内容。",
    ],
    styles,
    story,
  )

  section(
    "6. 邮件订阅与消息通知",
    [
      "完成日报邮件订阅功能，从最初的 GitHub Pages 静态实现升级为后端 API + 数据库 + 邮件发送链路。用户可提交邮箱订阅，系统保存订阅时间、发送状态，并在 monitor.nexo.hk 的业务运营页可视化查看和删除。",
      "实现订阅成功欢迎邮件、日报摘要邮件、日报生成失败通知邮件三类核心邮件能力，并支持中英文双语内容。还为已订阅用户补发过欢迎邮件。",
      "邮件发送能力已接入 Gmail 发信方案，相关发送结果、失败原因、当日日报是否已发送等状态都在数据库与后台管理页有留痕。",
    ],
    styles,
    story,
  )

  section(
    "7. 运维监控平台",
    [
      "完成 monitor.nexo.hk 独立运维站点建设，并通过 Google OAuth 实现登录保护；白名单已严格收敛到指定 Gmail 账号，且补强了 human verification 与退出后重新验证逻辑。",
      "运维看板目前覆盖页面访问量、输入 / 输出 / 总 Token 累计、分钟级趋势图、订阅用户管理、数据表单浏览、数据生成工具、股票数据后台等功能，并已适配电脑与手机端。",
      "Token 统计从最初的按天汇总，逐步优化为分钟级入库和分钟桶刷新；访问量统计也按相同逻辑处理，并统一调整为中国时区展示。",
    ],
    styles,
    story,
  )

  section(
    "8. AI 能力接入",
    [
      "已把 OpenAI 主模型接入到日报分析与 AI Assistant 中，并加入请求排队与限流，减少 429 频率。日报生成后可自动发送给 GPT 分析，并把简要摘要与详细解读分别用于 Dashboard 和 Daily Report 页面。",
      "在排查过程中处理过 OPENAI_API_KEY、配额、模型回退、分析内容未回写数据库、前端未展示 detailed_interpretation 等问题，并最终改为数据库存储后通过 API 提供给页面。",
      "除宏观日报外，AI 功能也扩展到了后台的数据问答与状态查询，用于辅助解释数据更新、失败指标和系统运行状态。",
    ],
    styles,
    story,
  )

  section(
    "9. 股票预测系统",
    [
      "根据用户提供的 PDF 需求与多份 PDD / UBER 等 CSV 数据，完成股票预测模块的端到端建设：数据导入、特征工程、随机森林训练、预测结果、回测、最新信号展示和后台管理工具。",
      "前台页面支持搜索 ticker、显示 Bullish / Bearish / Neutral 状态颜色、展示关键指标解释和两张趋势图的投资者视角说明。后台页面支持上传识别、进度条、文件识别结果、导入训练、训练历史与自动抓取。",
      "对 Yahoo 自动抓取链路处理了多类真实问题，包括 500、413、页面表格为空、训练超时、异步训练、防止接口阻塞、KO 与 HSDT 抓取失败等，系统鲁棒性已经显著提高。",
    ],
    styles,
    story,
  )

  section(
    "10. Polymarket 量化交易系统 MVP",
    [
      "依据《Polymarket 量化交易系统 PRD + 技术架构文档 v1.1》，已在当前仓库中开发并上线一套可运行的 MVP，包括账户接入、凭证派生、自动交易开关、市场 / 盘口存储、套利扫描、VWAP 仿真、风险总览、模拟执行与审计。",
      "新增了 polymarket.html 演示页面，以及一组 /api/v1 路径的后端接口，支持机会扫描、账户连接、风控查看、执行演练和回放摘要。代码已推送 GitHub 并同步部署到服务器。",
      "当前阶段仍属于非托管与模拟执行架构，尚未替代用户钱包签名；但数据库、接口和服务边界已按 PRD 预留了继续接入真实 Polymarket API / Signing Service / Account Sync 的基础。",
    ],
    styles,
    story,
  )

  section(
    "11. 线上部署与基础设施",
    [
      "完成网站从 GitHub Pages 向阿里云的逐步迁移，并最终实现前后端都部署在阿里云服务器，绑定 nexo.hk、monitor.nexo.hk、api.nexo.hk 等域名，处理了 HTTPS、DNS、Nginx、systemd、ECS 安全组等问题。",
      "建立了代码同步与发布流程：本地开发、GitHub 推送、服务器拉取部署、服务重启、线上 smoke test。期间也多次检查并纠正 GitHub 与服务器版本不一致的问题。",
      "对于服务器上自动任务与运行产物，已经形成保留现场、stash、上线验证和回滚意识，减少对现网数据与已生成报表的影响。",
    ],
    styles,
    story,
  )

  section(
    "12. 已解决的典型问题",
    [
      "修复过 Dashboard 与 Daily Report 中 14 维度缺失、指标值未显示、AI 解读内容不展示、手机端排版错乱、Monitor 页面风格不统一、重复 footer、语言切换混杂等一系列前端问题。",
      "修复过订阅接口不可用、后端未部署、邮件不发送、白名单失效、Google OAuth 后退出验证失效、human verification 返回首页绕过、Token 图表不按分钟展示、时间未按中国时区等运维问题。",
      "修复过股票模块上传 500 / 413、自动抓取失败、Yahoo 登录挑战、训练请求超时、日报分析未调用 GPT、FRED Key 接入、OpenRouter 动态内容抓取等数据与算法链路问题。",
    ],
    styles,
    story,
  )

  section(
    "13. 当前系统状态判断",
    [
      "当前整套系统已经从“静态页面 + 本地 Excel”演进到“线上站点 + 后端数据库 + 定时任务 + AI + 邮件 + 运维后台 + 多业务模块”的工程化状态。",
      "宏观监控主链路、订阅与邮件、运维监控、股票预测和 Polymarket MVP 都已有可运行版本，适合继续做稳定性增强、数据源扩展、真实交易接入和权限治理。",
      "后续若继续推进，最值得优先投入的方向是：提升外部数据源新鲜度保障、补强真实交易账户接入、完善回测与归因、以及把更多后台操作沉淀成可复用脚本与运维 SOP。",
    ],
    styles,
    story,
  )

  story.append(Spacer(1, 8))
  story.append(
    p(
      f"文档生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}（Asia/Shanghai）",
      styles["CNSmall"],
    )
  )
  story.append(
    p(
      "生成方式：项目仓库内脚本自动汇总生成。若后续继续开发，可重复运行本脚本刷新版本。",
      styles["CNSmall"],
    )
  )

  doc.build(story)
  print(OUTPUT_PATH)


if __name__ == "__main__":
  build_pdf()
