const DB_NAME = "macro-monitor-db";
const DB_VERSION = 1;
const STORAGE_KEY = "macro-monitor-model";
const LANG_KEY = "macro-monitor-lang";
const DEFAULT_MODEL_FILE = "./model.xlsx";
const STATIC_REPORT_INDEX = "./reports/index.json";
const STATIC_SNAPSHOT = "./data/latest_snapshot.json";
const STATIC_SUBSCRIBERS = "./data/subscribers.json";
const SUBSCRIPTION_ISSUE_URL = "https://github.com/Joeyxia/MacoEcoReport/issues/new";
const API_BASE_KEY = "macro-monitor-api-base";
const MIGRATED_KEY = "macro-monitor-db-migrated";
let dashboardHeavyRenderToken = 0;
let dashboardWorkbookObserver = null;
let dashboardWorkbookLoaded = false;
let dashboardWorkbookLoading = false;
let dashboardTableObservers = [];
let openrouterControlsBound = false;
const dashboardTableLoaded = {
  dimensions: false,
  inputs: false,
  indicators: false,
  scores: false,
  alerts: false
};
const dashboardPrefetchedTableRows = {};
let dashboardPrefetchStarted = false;

const i18n = {
  en: {
    brand_name: "Macro Risk Monitor",
    title_dashboard: "Macro Risk Monitor | Dashboard",
    title_daily_report: "Macro Risk Monitor | Daily Report",
    title_indicators: "Macro Risk Monitor | Indicators",
    title_glossary: "Macro Risk Monitor | Glossary",
    title_subscribe: "Macro Risk Monitor | Subscribe",
    title_ai_assistant: "Macro Risk Monitor | AI Assistant",
    title_openrouter: "Macro Risk Monitor | OpenRouter Rankings",
    title_portfolio_watchlist: "Macro Risk Monitor | Portfolio Watchlist",
    title_regime_transmission: "Macro Risk Monitor | Regime & Transmission",
    title_about: "Macro Risk Monitor | About Nexo",
    title_polymarket_page: "Macro Risk Monitor | Polymarket",
    title_polymarket_pnl: "Macro Risk Monitor | Polymarket PnL Dashboard",
    nav_dashboard: "Dashboard",
    nav_daily_report: "Daily Report",
    nav_indicators: "Indicators",
    nav_glossary: "Glossary",
    nav_subscribe: "Subscribe",
    nav_ai_assistant: "AI Assistant",
    nav_openrouter: "OpenRouter",
    nav_stock_prediction: "Stock Prediction",
    nav_polymarket: "Polymarket",
    nav_polymarket_pnl: "Polymarket PnL",
    nav_portfolio_watchlist: "Portfolio Watchlist",
    nav_regime_transmission: "Regime & Transmission",
    nav_intro: "About Nexo",
    about_eyebrow: "Platform Overview",
    about_page_title: "What Nexo Macro Intelligence Does",
    about_page_desc: "A 14-dimension macro warning system that turns daily data into regime diagnosis, transmission mapping, and investable actions.",
    about_block_1_title: "Core Functional Modules",
    about_block_1_desc: "The website is built around decision speed and explainability.",
    about_mod_1: "Dashboard: composite score, regime, alerts, top drivers, and key indicators.",
    about_mod_2: "Daily Report: structured narrative, AI interpretation, and archived daily links.",
    about_mod_3: "Indicators & Glossary: full indicator dictionary and model terminology.",
    about_mod_4: "Portfolio Watchlist: user watchlists, position mapping, and macro-linked risk output.",
    about_mod_5: "Regime & Transmission: regime engine, geopolitical overlay, action bias, and transmission heatmap.",
    about_mod_6: "Stock Prediction: ticker-level forecasts, backtest curves, and feature importance.",
    about_block_2_title: "Daily Operating Workflow",
    about_flow_1_title: "08:00 (China Time): Data Refresh",
    about_flow_1_desc: "Fetch latest indicator data from configured sources and validate freshness.",
    about_flow_2_title: "09:00 (China Time): Report Generation",
    about_flow_2_desc: "Run model scoring, produce daily report, and write outputs to database.",
    about_flow_3_title: "Post Report: Distribution & AI",
    about_flow_3_desc: "Send report emails, run AI interpretation, and sync summary back to dashboard.",
    about_block_3_title: "What You Get As A User",
    about_value_1: "A single macro regime view that is updated every day.",
    about_value_2: "Transparent logic from data -> signal -> action.",
    about_value_3: "Portfolio-level risk guidance connected to your own holdings.",
    about_value_4: "Bilingual interface (Chinese/English) for teams and clients.",
    about_block_4_title: "System Architecture (High Level)",
    about_arch: "Frontend pages on nexo.hk + Flask backend API + SQLite operational database + scheduled jobs + monitor console on monitor.nexo.hk.",
    section_regime_engine: "Regime Engine",
    section_geopolitical_overlay: "Geopolitical Overlay",
    section_action_bias: "Action Bias",
    section_portfolio_macro_risk: "Portfolio Macro Risk",
    section_sector_asset_transmission: "Sector / Asset Transmission",
    section_capital_warning_blocks: "Capital Market Warning Blocks",
    report_regime_block_title: "Macro Regime",
    report_transmission_block_title: "Market Transmission",
    report_portfolio_impact_block_title: "Portfolio Impact",
    report_action_bias_block_title: "Action Bias",
    report_overlay_block_title: "Geopolitical / Energy Shock",
    report_watchlist_block_title: "Watchlist Summary",
    stock_macro_exposure: "Macro Exposure",
    stock_macro_signal: "Macro Signal",
    pw_title: "Portfolio Watchlist",
    pw_eyebrow: "Portfolio Watchlist",
    pw_page_title: "Watchlist / Position Risk Mapping",
    pw_page_desc: "Create watchlists, add stocks, and read macro exposure, action bias, and vulnerable positions.",
    pw_email_placeholder: "email",
    pw_name_placeholder: "watchlist name",
    pw_create: "Create Watchlist",
    pw_positions: "Positions",
    pw_ticker_placeholder: "ticker",
    pw_qty_placeholder: "quantity",
    pw_add_position: "Add Position",
    pw_risk_summary: "Portfolio Risk Summary",
    rt_title: "Regime & Transmission",
    rt_eyebrow: "Regime & Transmission",
    rt_page_title: "Market Regime / Transmission Console",
    rt_page_desc: "View the latest regime, recent regime history, geopolitical overlay, transmission map, and action bias.",
    rt_current_regime: "Current Regime",
    rt_regime_history: "30 / 90 Day Regime History",
    rt_transmission_heatmap: "Transmission Heatmap",
    rt_regime_explain_title: "Regime Indicator Explanation",
    rt_overlay_explain_title: "Geopolitical Overlay Explanation",
    rt_action_explain_title: "Action Bias Explanation",
    rt_transmission_explain_title: "Transmission Heatmap Explanation",
    rt_investor_brief_title: "Daily Investor Brief",
    dashboard_eyebrow: "Global Macro Crisis Radar",
    dashboard_title: "Institutional 14-Dimension Monitoring Dashboard",
    dashboard_subtitle: "Upload your model workbook to refresh total score, dimension contribution, and warning signals.",
    load_model: "Load Model (.xlsx)",
    using_sample_data: "Using built-in sample data",
    macro_composite_score: "Macro Composite Score",
    as_of: "As of",
    triggered_alerts: "Triggered Alerts",
    top_dimension_contributors: "Top Dimension Contributors",
    primary_drivers: "Primary Drivers",
    key_indicators_overview: "Key Indicators Snapshot",
    latest_report_summary: "Latest Report Summary",
    daily_watch_items: "Daily Watch Items",
    dimensions_14_detail: "All 14 Dimensions Detail",
    dimensions_14_detail_desc: "This section shows complete dimension definitions, tiers, weights, and update frequencies.",
    model_core_tables: "Model Core Tables",
    model_core_tables_desc: "Complete structured data from your 14-dimension model.",
    dimensions: "Dimensions",
    inputs_latest: "Inputs (Latest)",
    indicators: "Indicators",
    scores: "Scores",
    alerts: "Alerts",
    workbook_explorer: "Workbook Data Explorer",
    workbook_explorer_desc: "Full raw content from every worksheet in the Excel file (all rows and columns).",
    need_today_note: "Need Today's Note?",
    need_today_note_desc: "Generate and edit a daily narrative from the current model snapshot.",
    open_daily_report: "Open Daily Report",
    daily_note: "Daily Intelligence Note",
    daily_report_title: "Macro Monitoring Daily Report",
    daily_report_desc: "Auto-drafted from the latest dashboard snapshot. Edit as needed, then save.",
    snapshot: "Snapshot",
    regenerate_draft: "Regenerate Draft",
    generate_final_report: "Generate Final Report",
    save: "Save",
    download_txt: "Download .txt",
    run_online_check: "Run online data check before final report",
    report_preview: "Report Preview (Reference Format)",
    daily_report_archive: "Daily Report Archive",
    daily_report_archive_desc: "Each saved day has a direct link.",
    online_check_results: "Online Data Check Results",
    detailed_indicator_scores: "Detailed Indicators Score",
    reference: "Reference",
    glossary_title: "Macro Terms Used in This System",
    glossary_desc: "Aligned to your 14-dimension monitoring framework and alert thresholds.",
    glossary_search_placeholder: "Search terms...",
    glossary_filter_all: "All Categories",
    glossary_filter_core_macro: "Core Macro",
    glossary_filter_policy_external: "Policy & External",
    glossary_filter_market_mapping: "Market Mapping",
    glossary_filter_theme_panel: "Theme Panel",
    glossary_filter_data_source: "Data Source",
    indicators_eyebrow: "Indicator Library",
    indicators_page_title: "All Indicators Information (from Indicators Sheet)",
    indicators_page_desc: "Full indicator definitions, scoring settings, data source links, and update frequencies.",
    all_indicators_info: "All Indicators Information"
    ,
    subscribe_title: "Email Subscription",
    subscribe_desc: "Subscribe to receive the daily report summary and link after 09:00 China time generation.",
    subscribe_email_label: "Email",
    subscribe_submit: "Subscribe",
    subscribe_note: "If backend is unavailable, subscription will fallback to GitHub request.",
    subscribe_count: "Active Subscribers",
    ai_assistant_page_title: "AI Data Assistant",
    ai_assistant_page_desc: "Ask natural-language questions about data freshness, failed indicators, and update status.",
    ai_data_assistant_title: "AI Data Assistant",
    ai_data_assistant_desc: "Ask natural-language questions about data freshness, failed indicators, and update status.",
    ai_data_assistant_placeholder: "e.g. Which indicators failed online verification today and what should I replace them with?",
    ai_data_assistant_submit: "Ask AI",
    openrouter_title: "OpenRouter Rankings",
    openrouter_desc: "Live snapshot from openrouter.ai/rankings, including top models and top apps.",
    openrouter_models: "Top Models",
    openrouter_apps: "Top Apps",
    openrouter_providers: "Top Providers",
    openrouter_prompts: "Top Prompts",
    openrouter_updated: "Fetched At",
    openrouter_source: "Source",
    openrouter_view_day: "Day",
    openrouter_view_week: "Week",
    openrouter_view_month: "Month",
    openrouter_view_all: "All",
    openrouter_category_all: "All Categories",
    openrouter_category_roleplay: "Roleplay",
    openrouter_category_coding: "Coding",
    openrouter_category_reasoning: "Reasoning",
    openrouter_category_translation: "Translation",
    openrouter_refresh: "Refresh",
    indicator_verification_status: "Indicator Verification Status",
    data_generated_at: "Data Generated At",
    signal_guide: "Signal Interpretation Guide",
    powered_by: "Powered by Nexo Marco Intelligence"
  },
  zh: {
    brand_name: "宏观风险监测",
    title_dashboard: "宏观风险监测 | 仪表盘",
    title_daily_report: "宏观风险监测 | 每日报告",
    title_indicators: "宏观风险监测 | 指标库",
    title_glossary: "宏观风险监测 | 术语表",
    title_subscribe: "宏观风险监测 | 订阅",
    title_ai_assistant: "宏观风险监测 | AI 助手",
    title_openrouter: "宏观风险监测 | OpenRouter 排行",
    title_portfolio_watchlist: "宏观风险监测 | 组合观察池",
    title_regime_transmission: "宏观风险监测 | 状态与传导",
    title_about: "宏观风险监测 | Nexo 功能介绍",
    title_polymarket_page: "宏观风险监测 | Polymarket",
    title_polymarket_pnl: "宏观风险监测 | Polymarket PnL账本看板",
    nav_dashboard: "仪表盘",
    nav_daily_report: "每日报告",
    nav_indicators: "指标库",
    nav_glossary: "术语表",
    nav_subscribe: "订阅",
    nav_ai_assistant: "AI 助手",
    nav_openrouter: "OpenRouter",
    nav_stock_prediction: "股票预测",
    nav_polymarket: "Polymarket",
    nav_polymarket_pnl: "Polymarket PnL账本",
    nav_portfolio_watchlist: "组合观察池",
    nav_regime_transmission: "状态与传导",
    nav_intro: "功能介绍",
    about_eyebrow: "平台总览",
    about_page_title: "Nexo Macro Intelligence 能做什么",
    about_page_desc: "一个 14 维宏观预警系统，把每日数据转成状态判断、传导映射与可执行建议。",
    about_block_1_title: "核心功能模块",
    about_block_1_desc: "网站围绕“决策速度 + 可解释性”构建。",
    about_mod_1: "仪表盘：综合评分、市场状态、预警、核心驱动与关键指标。",
    about_mod_2: "每日报告：结构化解读、AI 报告分析与日报归档链接。",
    about_mod_3: "指标库与术语表：完整指标定义和模型术语解释。",
    about_mod_4: "组合观察池：自定义观察池、持仓映射与宏观联动风险输出。",
    about_mod_5: "状态与传导：状态引擎、地缘叠加层、动作偏向与传导热力表。",
    about_mod_6: "股票预测：股票级预测、回测曲线与特征重要性。",
    about_block_2_title: "每日运行流程",
    about_flow_1_title: "北京时间 08:00：数据刷新",
    about_flow_1_desc: "从配置数据源抓取最新指标，并执行新鲜度校验。",
    about_flow_2_title: "北京时间 09:00：日报生成",
    about_flow_2_desc: "执行模型打分，生成每日报告，并写入数据库。",
    about_flow_3_title: "报告后：分发与 AI 解析",
    about_flow_3_desc: "发送邮件摘要、执行 AI 解读，并同步摘要回仪表盘。",
    about_block_3_title: "你可以获得什么",
    about_value_1: "每日更新的一体化宏观状态视图。",
    about_value_2: "从数据 -> 信号 -> 动作的透明逻辑链。",
    about_value_3: "与自身持仓绑定的组合风险建议。",
    about_value_4: "支持中英文切换，适配团队协同与客户沟通。",
    about_block_4_title: "系统架构（高层）",
    about_arch: "nexo.hk 前端页面 + Flask 后端 API + SQLite 业务数据库 + 定时任务 + monitor.nexo.hk 运维控制台。",
    section_regime_engine: "状态引擎",
    section_geopolitical_overlay: "地缘政治叠加层",
    section_action_bias: "动作偏向",
    section_portfolio_macro_risk: "组合宏观风险",
    section_sector_asset_transmission: "板块 / 资产传导",
    section_capital_warning_blocks: "资本市场预警模块",
    report_regime_block_title: "宏观状态",
    report_transmission_block_title: "市场传导",
    report_portfolio_impact_block_title: "持仓影响",
    report_action_bias_block_title: "动作建议",
    report_overlay_block_title: "地缘政治与能源冲击专项",
    report_watchlist_block_title: "观察池影响摘要",
    stock_macro_exposure: "宏观暴露",
    stock_macro_signal: "宏观信号",
    pw_title: "组合观察池",
    pw_eyebrow: "组合观察池",
    pw_page_title: "观察池 / 持仓风险映射",
    pw_page_desc: "创建观察池、加入股票，并查看宏观暴露、动作偏向和脆弱持仓。",
    pw_email_placeholder: "邮箱",
    pw_name_placeholder: "观察池名称",
    pw_create: "创建观察池",
    pw_positions: "持仓",
    pw_ticker_placeholder: "股票代码",
    pw_qty_placeholder: "数量",
    pw_add_position: "添加持仓",
    pw_risk_summary: "组合风险摘要",
    rt_title: "状态与传导",
    rt_eyebrow: "状态与传导",
    rt_page_title: "市场状态 / 传导控制台",
    rt_page_desc: "查看最新宏观状态、近阶段历史、地缘政治叠加、传导图谱和动作偏向。",
    rt_current_regime: "当前状态",
    rt_regime_history: "30 / 90 天状态历史",
    rt_transmission_heatmap: "传导热力表",
    rt_regime_explain_title: "状态指标解读",
    rt_overlay_explain_title: "地缘政治叠加层解读",
    rt_action_explain_title: "动作偏向解读",
    rt_transmission_explain_title: "传导热力表解读",
    rt_investor_brief_title: "当日投资人说明",
    dashboard_eyebrow: "全球宏观危机雷达",
    dashboard_title: "14维机构级宏观监控仪表盘",
    dashboard_subtitle: "上传模型工作簿后，可刷新总分、维度贡献和预警信号。",
    load_model: "加载模型（.xlsx）",
    using_sample_data: "使用内置样例数据",
    macro_composite_score: "宏观综合评分",
    as_of: "更新日",
    triggered_alerts: "触发预警",
    top_dimension_contributors: "维度贡献 Top",
    primary_drivers: "核心驱动",
    key_indicators_overview: "关键指标概览",
    latest_report_summary: "最新报告摘要",
    daily_watch_items: "当日关注项",
    dimensions_14_detail: "14个维度信息",
    dimensions_14_detail_desc: "展示维度定义、层级、权重和更新频率。",
    model_core_tables: "模型核心数据表",
    model_core_tables_desc: "完整展示你的14维模型结构化数据。",
    dimensions: "维度",
    inputs_latest: "Inputs（最新）",
    indicators: "指标",
    scores: "评分",
    alerts: "预警",
    workbook_explorer: "工作簿全量浏览",
    workbook_explorer_desc: "展示 Excel 每个工作表的全部行列原始数据。",
    need_today_note: "需要今日简报？",
    need_today_note_desc: "基于当前模型快照自动生成并编辑每日报告。",
    open_daily_report: "打开每日报告",
    daily_note: "每日情报",
    daily_report_title: "宏观监控每日报告",
    daily_report_desc: "根据最新仪表盘自动起草，可编辑后保存。",
    snapshot: "快照",
    regenerate_draft: "重新生成草稿",
    generate_final_report: "生成最终报告",
    save: "保存",
    download_txt: "下载 .txt",
    run_online_check: "生成最终报告前执行在线数据校验",
    report_preview: "报告预览（参考格式）",
    daily_report_archive: "每日报告归档",
    daily_report_archive_desc: "每一天报告都生成可访问链接。",
    online_check_results: "在线数据校验结果",
    detailed_indicator_scores: "指标详细评分",
    reference: "参考",
    glossary_title: "系统术语说明",
    glossary_desc: "与14维监控框架和预警阈值保持一致。",
    glossary_search_placeholder: "搜索术语...",
    glossary_filter_all: "全部分类",
    glossary_filter_core_macro: "核心宏观",
    glossary_filter_policy_external: "政策与外部",
    glossary_filter_market_mapping: "市场映射",
    glossary_filter_theme_panel: "主题面板",
    glossary_filter_data_source: "数据来源",
    indicators_eyebrow: "指标信息库",
    indicators_page_title: "全部指标信息（来自 Indicators 表）",
    indicators_page_desc: "完整展示指标定义、评分参数、数据源和更新频率。",
    all_indicators_info: "全部指标信息",
    subscribe_title: "邮件订阅",
    subscribe_desc: "每日北京时间09:00生成报告后，向订阅邮箱发送摘要与报告链接。",
    subscribe_email_label: "邮箱",
    subscribe_submit: "订阅",
    subscribe_note: "若后端不可用，将自动回退到 GitHub 请求订阅。",
    subscribe_count: "当前有效订阅数",
    ai_assistant_page_title: "AI 数据助手",
    ai_assistant_page_desc: "可以直接询问数据新鲜度、失败指标和更新状态。",
    ai_data_assistant_title: "AI 数据助手",
    ai_data_assistant_desc: "可以直接询问数据新鲜度、失败指标和更新状态。",
    ai_data_assistant_placeholder: "例如：今天哪些指标在线校验失败？分别建议替代数据源是什么？",
    ai_data_assistant_submit: "AI 查询",
    openrouter_title: "OpenRouter 排行",
    openrouter_desc: "来自 openrouter.ai/rankings 的实时快照，展示 Top Models 与 Top Apps。",
    openrouter_models: "热门模型",
    openrouter_apps: "热门应用",
    openrouter_providers: "热门提供方",
    openrouter_prompts: "热门提示词",
    openrouter_updated: "抓取时间",
    openrouter_source: "来源",
    openrouter_view_day: "日",
    openrouter_view_week: "周",
    openrouter_view_month: "月",
    openrouter_view_all: "全部",
    openrouter_category_all: "全部分类",
    openrouter_category_roleplay: "角色扮演",
    openrouter_category_coding: "编程",
    openrouter_category_reasoning: "推理",
    openrouter_category_translation: "翻译",
    openrouter_refresh: "刷新",
    indicator_verification_status: "指标在线校验状态",
    data_generated_at: "数据生成时间",
    signal_guide: "信号解读指南",
    powered_by: "由 Nexo Marco Intelligence 提供支持"
  }
};

const dimensionNameMap = {
  D01: { zh: "货币政策与流动性", en: "Monetary Policy & Liquidity" },
  D02: { zh: "增长与前瞻", en: "Growth & Forward Signals" },
  D03: { zh: "通胀与价格压力", en: "Inflation & Price Pressure" },
  D04: { zh: "就业与居民部门", en: "Labor & Households" },
  D05: { zh: "企业盈利与信用", en: "Earnings & Credit" },
  D06: { zh: "房地产与利率敏感部门", en: "Housing & Rate-sensitive Sectors" },
  D07: { zh: "风险偏好与跨资产波动", en: "Risk Appetite & Cross-asset Volatility" },
  D08: { zh: "外部部门与美元条件", en: "External Sector & Dollar Conditions" },
  D09: { zh: "财政政策与债务约束", en: "Fiscal Policy & Debt Constraint" },
  D10: { zh: "金融条件与信用传导", en: "Financial Conditions & Credit Transmission" },
  D11: { zh: "大宗商品与能源/地缘风险", en: "Commodities & Geopolitical Risk" },
  D12: { zh: "信心与不确定性", en: "Confidence & Uncertainty" },
  D13: { zh: "AI资本开支周期（主题）", en: "AI Capex Cycle (Theme)" },
  D14: { zh: "加密与稳定币流动性（主题）", en: "Crypto & Stablecoin Liquidity (Theme)" }
};

const tierMap = {
  "Core Macro": { zh: "核心宏观", en: "Core Macro" },
  "Market Mapping": { zh: "市场映射", en: "Market Mapping" },
  "Policy & External": { zh: "政策与外部", en: "Policy & External" },
  "Policy&External": { zh: "政策与外部", en: "Policy & External" },
  "External/Commodities": { zh: "外部/大宗商品", en: "External/Commodities" },
  "Soft Data": { zh: "软数据", en: "Soft Data" },
  "Theme Panel": { zh: "主题面板", en: "Theme Panel" }
};

const statusMap = {
  "扩张偏热": "Expansion Overheating",
  "温和扩张": "Moderate Expansion",
  "中性偏脆弱": "Neutral Fragile",
  "防御区": "Defensive",
  "衰退/危机": "Recession/Crisis"
};

const indicatorNameMap = {
  "10Y-3M利差（bps）": "10Y-3M Spread (bps)",
  "SOFR（%）": "SOFR (%)",
  "美联储总资产（$T）": "Fed Total Assets ($T)",
  "实际GDP环比折年（%）": "Real GDP SAAR (%)",
  "制造业PMI": "Manufacturing PMI",
  "初请失业金四周均值": "Initial Claims 4WMA",
  "核心CPI同比（%）": "Core CPI YoY (%)",
  "核心PCE同比（%）": "Core PCE YoY (%)",
  "5Y5Y通胀预期（%）": "5Y5Y Inflation Expectation (%)",
  "失业率（%）": "Unemployment Rate (%)",
  "平均时薪同比（%）": "Average Hourly Earnings YoY (%)",
  "信用卡拖欠率（%）": "Credit Card Delinquency Rate (%)",
  "标普500 Forward P/E（x）": "S&P 500 Forward P/E (x)",
  "EPS修正广度（上调-下调，%）": "EPS Revision Breadth (Up-Down, %)",
  "高收益信用利差OAS（bps）": "HY Credit Spread OAS (bps)",
  "30年按揭利率（%）": "30Y Mortgage Rate (%)",
  "成屋销售折年（百万套）": "Existing Home Sales SAAR (mn)",
  "新屋开工（百万套）": "Housing Starts (mn)",
  "VIX": "VIX",
  "MOVE": "MOVE",
  "标普500近3个月最大回撤（%）": "S&P 500 Max Drawdown 3M (%)",
  "美元指数DXY": "US Dollar Index (DXY)",
  "美日10Y利差（bps）": "US-JP 10Y Spread (bps)",
  "海外净买入美国资产（自定义）": "Foreign Net Buying of US Assets (custom)",
  "财政赤字/GDP（%）": "Fiscal Deficit/GDP (%)",
  "债务/GDP（%）": "Debt/GDP (%)",
  "利息支出/财政收入（%）": "Interest Expense/Fiscal Revenue (%)",
  "金融条件指数（FCI）": "Financial Conditions Index (FCI)",
  "银行信贷增速（% YoY）": "Bank Credit Growth (% YoY)",
  "TED利差（bps）": "TED Spread (bps)",
  "WTI原油（$/桶）": "WTI Crude ($/bbl)",
  "大宗商品指数（CRB）同比（%）": "CRB Commodity Index YoY (%)",
  "地缘政治风险指数（GPR）": "Geopolitical Risk Index (GPR)",
  "消费者信心指数": "Consumer Confidence Index",
  "CEO Confidence": "CEO Confidence",
  "经济政策不确定性（EPU）": "Economic Policy Uncertainty (EPU)",
  "云业务增速（加权，% YoY）": "Cloud Revenue Growth (weighted, % YoY)",
  "AI相关Capex指引（自定义）": "AI-related Capex Guidance (custom)",
  "半导体景气代理（Book-to-Bill）": "Semiconductor Proxy (Book-to-Bill)",
  "BTC价格（$）": "BTC Price ($)",
  "USDC市值（$B）": "USDC Market Cap ($B)",
  "稳定币总市值（$B）": "Total Stablecoin Market Cap ($B)"
};

const sampleModel = {
  asOf: "2026-03-01",
  totalScore: 58.4,
  status: "Neutral Fragile",
  alerts: [{ id: "A03", level: "YELLOW", condition: "10Y-3M < -50bps", triggered: true }],
  dimensions: [
    { name: "Monetary Policy & Liquidity", score: 46.2, contribution: 5.5 },
    { name: "Growth & Forward Signals", score: 57.8, contribution: 6.4 },
    { name: "Inflation & Price Pressure", score: 61.3, contribution: 6.1 }
  ],
  drivers: [
    { title: "Primary Support", text: "Core inflation has moderated toward the target zone." },
    { title: "Primary Drag", text: "Yield curve inversion and liquidity tightness still cap risk appetite." },
    { title: "Risk Trigger", text: "A volatility spike can quickly shift the regime to defense." }
  ],
  tables: { dimensions: [], indicators: [], inputs: [], scores: [], alerts: [] },
  workbook: { sheets: [] },
  onlineCheck: []
};

const glossaryTerms = [
  {
    en: {
      term: "Macro Composite Score",
      desc: "Weighted 0-100 aggregate score across the 14 dimensions.",
      why: "It is the top-level regime signal used for strategic risk stance.",
      read: "75+ overheated expansion; 60-75 moderate expansion; 45-60 fragile neutral; 30-45 defensive; below 30 crisis.",
      use: "Use score trend (not one-point value) to size risk budgets."
    },
    zh: {
      term: "宏观综合评分",
      desc: "14个维度加权后的0-100综合分。",
      why: "它是用于战略风险配置的顶层状态信号。",
      read: "75以上偏热扩张；60-75温和扩张；45-60中性偏脆弱；30-45防御；30以下危机。",
      use: "建议看趋势而不是单日点位，用于调整组合风险预算。"
    }
  },
  {
    en: {
      term: "Alert Trigger",
      desc: "Threshold-based risk event (for example VIX > 30).",
      why: "Alerts provide tactical risk control signals that react faster than smoothed scores.",
      read: "RED means immediate stress, YELLOW means rising fragility, no trigger means thresholds are stable.",
      use: "Use alerts for hedging and gross/net exposure adjustments."
    },
    zh: {
      term: "预警触发",
      desc: "基于阈值触发的风险事件（例如 VIX > 30）。",
      why: "预警比平滑后的总分更快，适合战术风控。",
      read: "红灯代表即时压力，黄灯代表脆弱性上升，无触发代表阈值内稳定。",
      use: "可用于对冲比例和仓位暴露的快速调整。"
    }
  },
  {
    en: {
      term: "TargetBand",
      desc: "Scoring mode where a defined value range receives highest score.",
      why: "Many macro variables are healthiest in a middle range instead of monotonic higher/lower.",
      read: "Inside band = high score; outside band decays toward worst bounds.",
      use: "Typical for inflation, unemployment, and policy-sensitive variables."
    },
    zh: {
      term: "TargetBand",
      desc: "目标区间评分：数值落在区间内得分最高。",
      why: "许多宏观变量并非越高越好或越低越好，而是存在最优中枢。",
      read: "落在目标区间内高分，偏离后向最差边界线性衰减。",
      use: "常用于通胀、失业率、政策敏感型变量。"
    }
  },
  {
    en: {
      term: "WeightedContribution",
      desc: "Dimension contribution after dimension weight is applied.",
      why: "It identifies what truly drives headline score changes.",
      read: "High score with low weight can contribute less than medium score with high weight.",
      use: "Track contribution deltas to explain day-over-day regime shifts."
    },
    zh: {
      term: "加权贡献",
      desc: "维度分乘以维度权重后的总分贡献。",
      why: "它能定位真正推动总分变化的来源。",
      read: "低权重高分维度可能贡献小于高权重中等分维度。",
      use: "可用于解释日报中总分变动的核心原因。"
    }
  },
  {
    en: {
      term: "HY OAS",
      desc: "US high-yield option-adjusted spread, a credit stress gauge.",
      why: "It is a direct market-implied proxy for default and refinancing pressure.",
      read: "Widening spread usually means tighter financial conditions and weaker risk appetite.",
      use: "Use together with VIX/MOVE for cross-asset stress confirmation."
    },
    zh: {
      term: "HY OAS",
      desc: "美国高收益债 OAS，反映信用压力。",
      why: "它直接反映违约与再融资压力的市场定价。",
      read: "利差走阔通常意味着金融条件收紧、风险偏好下降。",
      use: "建议与VIX/MOVE联动观察确认跨资产压力。"
    }
  },
  {
    en: {
      term: "Net Liquidity Proxy",
      desc: "Common proxy: Fed assets - TGA - RRP.",
      why: "It approximates system liquidity available to risk assets.",
      read: "Rising proxy often supports risk assets; falling proxy can tighten market breadth.",
      use: "Compare with equity/credit drawdowns to detect liquidity-led risk events."
    },
    zh: {
      term: "净流动性代理",
      desc: "常用口径：美联储资产 - TGA - RRP。",
      why: "它近似反映可流向风险资产的系统流动性。",
      read: "上行通常支撑风险资产，下行可能对应广度收缩和估值压力。",
      use: "可与股债回撤联动判断是否为流动性主导的风险事件。"
    }
  }
];

function getLang() {
  const stored = localStorage.getItem(LANG_KEY);
  return stored === "zh" ? "zh" : "en";
}

function setLang(lang) {
  localStorage.setItem(LANG_KEY, lang);
  document.documentElement.lang = lang === "zh" ? "zh-CN" : "en";
}

function t(key) {
  const lang = getLang();
  return i18n[lang]?.[key] || i18n.en[key] || "";
}

function localizeStatus(text) {
  const raw = asText(text);
  if (!raw) return raw;
  if (getLang() === "zh") {
    const zh = Object.entries(statusMap).find(([, en]) => en.toLowerCase() === raw.toLowerCase());
    return zh ? zh[0] : raw;
  }
  return statusMap[raw] || raw;
}

function normalizeDimensionIdFromName(name) {
  const n = asText(name);
  const matchId = n.match(/\b(D\d{2})\b/i);
  if (matchId) return matchId[1].toUpperCase();
  for (const [id, label] of Object.entries(dimensionNameMap)) {
    if (n === label.zh || n === label.en) return id;
  }
  return "";
}

function localizeDimensionName(name, idHint = "") {
  const raw = asText(name);
  const id = asText(idHint) || normalizeDimensionIdFromName(name);
  const mapped = dimensionNameMap[id];
  if (!mapped) return raw;
  const label = getLang() === "zh" ? mapped.zh : mapped.en;
  return /^D\d{2}\b/i.test(raw) ? `${id} ${label}` : label;
}

function localizeTierName(name) {
  const raw = asText(name);
  const mapped = tierMap[raw];
  if (!mapped) return raw;
  return getLang() === "zh" ? mapped.zh : mapped.en;
}

function localizeIndicatorName(name) {
  const raw = asText(name);
  if (!raw) return raw;
  if (getLang() === "zh") {
    const zh = Object.entries(indicatorNameMap).find(([, en]) => en.toLowerCase() === raw.toLowerCase());
    return zh ? zh[0] : raw;
  }
  return indicatorNameMap[raw] || raw;
}

function containsChinese(text) {
  return /[\u4e00-\u9fff]/.test(asText(text));
}

function localizeDimensionDefinition(id, raw) {
  const text = asText(raw);
  if (getLang() === "zh") return text;
  if (!containsChinese(text)) return text;
  const fallback = {
    D01: "Yield curve, short-end funding, central bank balance sheet, and net liquidity.",
    D02: "Forward growth indicators such as GDP, PMI, orders, and claims.",
    D03: "Core inflation and inflation expectations to infer policy/profit pressure.",
    D04: "Labor, income, and household credit stress.",
    D05: "Earnings revisions and credit-spread/default stress.",
    D06: "Mortgage rates, transactions, and housing starts as rate transmission channels.",
    D07: "Market risk mapping via VIX, MOVE, and drawdown metrics.",
    D08: "DXY, cross-rate spreads, and capital-flow based external financing conditions.",
    D09: "Deficit, interest burden, and debt trajectory.",
    D10: "Financial conditions index, bank credit supply, and funding pressure.",
    D11: "Commodity and geopolitical shocks to inflation and growth.",
    D12: "Consumer/business confidence and policy uncertainty.",
    D13: "Cloud/AI capex and revenue momentum thematic monitor.",
    D14: "Stablecoin and on-chain liquidity thematic monitor."
  };
  return fallback[id] || text;
}

function applyI18n() {
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.getAttribute("data-i18n");
    if (!node.dataset.i18nDefault) {
      node.dataset.i18nDefault = node.textContent || "";
    }
    const value = t(key);
    node.textContent = value || node.dataset.i18nDefault || "";
  });
  document.querySelectorAll("[data-i18n-placeholder]").forEach((node) => {
    const key = node.getAttribute("data-i18n-placeholder");
    if (!node.dataset.i18nPlaceholderDefault) {
      node.dataset.i18nPlaceholderDefault = node.getAttribute("placeholder") || "";
    }
    const value = t(key);
    node.setAttribute("placeholder", value || node.dataset.i18nPlaceholderDefault || "");
  });

  const toggle = document.getElementById("lang-toggle");
  if (toggle) toggle.textContent = getLang() === "zh" ? "EN" : "中文";
}

function getApiBase() {
  const fromStorage = asText(localStorage.getItem(API_BASE_KEY));
  if (fromStorage) return fromStorage.replace(/\/+$/, "");
  const meta = document.querySelector('meta[name="macro-api-base"]');
  const fromMeta = asText(meta?.content);
  if (fromMeta) return fromMeta.replace(/\/+$/, "");
  if (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1") return "http://127.0.0.1:5000";
  return "";
}

function trackPublicPageView() {
  const payload = {
    path: `${window.location.pathname}${window.location.search || ""}`,
    referrer: document.referrer || ""
  };
  const body = JSON.stringify(payload);
  try {
    const url = "https://monitor.nexo.hk/monitor-api/track/page";
    if (navigator.sendBeacon) {
      navigator.sendBeacon(url, new Blob([body], { type: "application/json" }));
      return;
    }
    fetch(url, { method: "POST", mode: "cors", headers: { "Content-Type": "application/json" }, body }).catch(() => {});
  } catch {}
}

async function apiFetch(path, options = {}) {
  const rawPath = asText(path);
  if (!rawPath) return null;
  const isAbsolute = /^https?:\/\//i.test(rawPath);
  const isApiPath = rawPath.startsWith("/api/");
  const base = getApiBase();
  if (!isAbsolute && !isApiPath && !base) return null;
  const url = isAbsolute ? rawPath : (isApiPath ? rawPath : `${base}${rawPath}`);
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  try {
    const res = await fetch(url, { ...options, headers, credentials: "include" });
    if (res.status === 204) return { ok: true };
    let body = null;
    try {
      body = await res.json();
    } catch {
      body = null;
    }
    if (!res.ok) {
      return body || { ok: false, error: `http_${res.status}` };
    }
    return body || { ok: true };
  } catch {
    return null;
  }
}

function setupLangToggle(onChange) {
  const btn = document.getElementById("lang-toggle");
  if (!btn) return;

  btn.addEventListener("click", () => {
    const next = getLang() === "zh" ? "en" : "zh";
    setLang(next);
    applyI18n();
    if (onChange) onChange();
  });
}

function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains("model")) db.createObjectStore("model", { keyPath: "id" });
      if (!db.objectStoreNames.contains("reports")) db.createObjectStore("reports", { keyPath: "date" });
      if (!db.objectStoreNames.contains("checks")) db.createObjectStore("checks", { keyPath: "id" });
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

async function dbPut(store, value) {
  const db = await openDB();
  await new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readwrite");
    tx.objectStore(store).put(value);
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error);
  });
  db.close();
}

async function dbGet(store, key) {
  const db = await openDB();
  const result = await new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const req = tx.objectStore(store).get(key);
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
  db.close();
  return result;
}

async function dbGetAll(store) {
  const db = await openDB();
  const result = await new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const req = tx.objectStore(store).getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
  db.close();
  return result;
}

function loadModelFallback() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) return sampleModel;
  try {
    return { ...sampleModel, ...JSON.parse(raw) };
  } catch {
    return sampleModel;
  }
}

function saveModelFallback(model) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(model));
}

async function loadCurrentModel(options = {}) {
  const view = asText(options.view).toLowerCase();
  const query = view === "core" ? "?view=core" : "";
  const fromApi = await apiFetch(`/api/model/current${query}`);
  if (fromApi) return normalizeModel({ ...sampleModel, ...fromApi });
  if (view === "core") return normalizeModel({ ...sampleModel, ...loadModelFallback() });
  const snapshot = await loadStaticSnapshot();
  if (snapshot) return normalizeModel({ ...sampleModel, ...snapshot });
  const fromDb = await dbGet("model", "current");
  if (fromDb?.payload) return normalizeModel(fromDb.payload);
  return normalizeModel(loadModelFallback());
}

async function loadDashboardSummary() {
  const fromApi = await apiFetch("/api/model/summary");
  return fromApi && !fromApi.error ? fromApi : null;
}

async function loadDashboardTables() {
  const fromApi = await apiFetch("/api/model/tables");
  if (fromApi && fromApi.tables) return fromApi.tables;
  const fallback = await loadCurrentModel();
  return fallback?.tables || {};
}

async function loadDashboardSingleTable(tableName) {
  const fromApi = await apiFetch(`/api/model/table/${encodeURIComponent(tableName)}`);
  if (fromApi && Array.isArray(fromApi.rows)) return fromApi.rows;
  const fallback = await loadDashboardTables();
  return fallback?.[tableName] || [];
}

async function loadDashboardWorkbook() {
  const fromApi = await apiFetch("/api/model/workbook");
  if (fromApi?.workbook) return fromApi.workbook;
  const fallback = await loadCurrentModel();
  return fallback?.workbook || { sheets: [] };
}

async function saveCurrentModel(model) {
  const normalized = normalizeModel(model);
  await apiFetch("/api/model/current", { method: "POST", body: JSON.stringify(normalized) });
  saveModelFallback(normalized);
  await dbPut("model", { id: "current", payload: normalized, updatedAt: new Date().toISOString() });
}

async function saveReport(date, text, meta, extra = {}) {
  await apiFetch("/api/reports", {
    method: "POST",
    body: JSON.stringify({
      date,
      text,
      meta,
      path: `reports/${date}.html`,
      reportPayload: extra.reportPayload,
      aiAnalysis: extra.aiAnalysis
    })
  });
  await dbPut("reports", { date, text, meta, updatedAt: new Date().toISOString() });
}

async function loadReport(date) {
  const fromApi = await apiFetch(`/api/reports/${encodeURIComponent(date)}`);
  if (fromApi) return fromApi;
  const local = await dbGet("reports", date);
  if (local) return local;
  const staticReports = await loadStaticReports();
  return staticReports.find((r) => r.date === date) || null;
}

async function loadReportAnalysis(date) {
  const fromApi = await apiFetch(`/api/reports/${encodeURIComponent(date)}/analysis`);
  return fromApi && !fromApi.error ? fromApi : null;
}

async function listReports(limit = 400) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 400, 1000));
  const fromApi = await apiFetch(`/api/reports?limit=${safeLimit}`);
  if (Array.isArray(fromApi?.reports)) return fromApi.reports.sort((a, b) => b.date.localeCompare(a.date));
  const reports = await dbGetAll("reports");
  const staticReports = await loadStaticReports();
  const merged = new Map();
  [...staticReports, ...reports].forEach((r) => {
    if (r?.date) merged.set(r.date, r);
  });
  return [...merged.values()].sort((a, b) => b.date.localeCompare(a.date));
}

async function loadStaticReports() {
  try {
    const res = await fetch(STATIC_REPORT_INDEX, { cache: "no-cache" });
    if (!res.ok) return [];
    const payload = await res.json();
    return Array.isArray(payload?.reports) ? payload.reports : [];
  } catch {
    return [];
  }
}

async function loadStaticSnapshot() {
  try {
    const res = await fetch(STATIC_SNAPSHOT, { cache: "no-cache" });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

async function loadStaticSubscribers() {
  const fromApi = await apiFetch("/api/subscribers");
  if (Array.isArray(fromApi?.subscribers)) return fromApi.subscribers;
  try {
    const res = await fetch(STATIC_SUBSCRIBERS, { cache: "no-cache" });
    if (!res.ok) return [];
    const payload = await res.json();
    if (!Array.isArray(payload?.subscribers)) return [];
    return payload.subscribers.filter((s) => asText(s.status).toLowerCase() === "active");
  } catch {
    return [];
  }
}

async function migrateBrowserDataToServer() {
  const base = getApiBase();
  if (!base) return;
  if (localStorage.getItem(MIGRATED_KEY) === "1") return;
  const modelRow = await dbGet("model", "current");
  const reports = await dbGetAll("reports");
  const checks = await dbGetAll("checks");
  const hasAny = !!(modelRow?.payload || reports.length || checks.length);
  if (!hasAny) {
    localStorage.setItem(MIGRATED_KEY, "1");
    return;
  }
  const payload = {
    model: modelRow?.payload || null,
    reports,
    checks
  };
  const res = await apiFetch("/api/migrate", { method: "POST", body: JSON.stringify(payload) });
  if (res?.ok) localStorage.setItem(MIGRATED_KEY, "1");
}

function asText(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeInputsTable(rows, fallbackAsOf = "") {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return [];
  if (Object.keys(list[0] || {}).some((k) => k.toLowerCase().includes("indicatorcode"))) return list;

  const firstKeys = Object.keys(list[0] || {});
  if (firstKeys.length !== 2) return list;

  const codeCol = firstKeys[0];
  const valueCol = firstKeys[1];
  const headerRow = list.find(
    (r) =>
      asText(r[codeCol]).toLowerCase() === "indicatorcode" &&
      asText(r[valueCol]).toLowerCase().includes("latestvalue")
  );
  if (!headerRow) return list;

  const asOfFromCol = /^\d{4}-\d{2}-\d{2}$/.test(asText(valueCol)) ? asText(valueCol) : "";
  const out = [];
  list.forEach((r) => {
    const code = asText(r[codeCol]);
    if (!/^[A-Z][A-Z0-9_]{1,40}$/i.test(code) || code.toLowerCase() === "indicatorcode") return;
    out.push({
      IndicatorCode: code,
      LatestValue: r[valueCol],
      ValueDate: asOfFromCol || fallbackAsOf
    });
  });
  return out.length ? out : list;
}

function normalizeModel(model) {
  const next = { ...model };
  next.tables = { ...(model.tables || {}) };
  next.tables.inputs = normalizeInputsTable(next.tables.inputs || [], next.asOf || "");
  return next;
}

function asNumber(v) {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const parsed = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function round(v, d = 1) {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  const p = 10 ** d;
  return Math.round(n * p) / p;
}

function escapeHtml(input) {
  return asText(input)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function findSheet(workbook, candidates) {
  const names = workbook.SheetNames || [];
  const normalized = names.map((name) => ({ name, lower: name.toLowerCase() }));
  for (const c of candidates) {
    const hit = normalized.find((item) => item.lower.includes(c));
    if (hit) return hit.name;
  }
  return null;
}

function keyByIncludes(row, includes) {
  const keys = Object.keys(row || {});
  for (const key of keys) {
    const lower = key.toLowerCase();
    if (includes.some((s) => lower.includes(s))) return key;
  }
  return null;
}

function isIndicatorLikeCode(value) {
  return /^[A-Z][A-Z0-9_]{1,40}$/i.test(asText(value));
}

function pickInputCodeKey(inputs) {
  if (!Array.isArray(inputs) || !inputs.length) return null;
  for (const row of inputs) {
    const key = keyByIncludes(row || {}, ["indicatorcode", "code", "指标编码"]);
    if (key) return key;
  }
  const keys = [...new Set(inputs.flatMap((r) => Object.keys(r || {})))];
  let best = null;
  let bestScore = -1;
  keys.forEach((k) => {
    let score = 0;
    inputs.forEach((row) => {
      const v = asText((row || {})[k]);
      if (!v) return;
      if (v.toLowerCase() === "indicatorcode") score += 20;
      else if (isIndicatorLikeCode(v)) score += 1;
    });
    if (score > bestScore) {
      bestScore = score;
      best = k;
    }
  });
  return bestScore > 0 ? best : null;
}

function pickInputValueKey(inputs) {
  if (!Array.isArray(inputs) || !inputs.length) return null;
  const codeKey = pickInputCodeKey(inputs);
  const keys = [...new Set(inputs.flatMap((r) => Object.keys(r || {})))];
  const explicit = keyByIncludes(inputs[0] || {}, ["latestvalue", "value", "最新值", "数值"]);
  if (explicit && explicit !== codeKey) return explicit;
  const dateHeader = keys.find((k) => /^\d{4}-\d{2}-\d{2}$/.test(asText(k)));
  if (dateHeader && dateHeader !== codeKey) return dateHeader;
  let best = null;
  let bestScore = -1;
  keys
    .filter((k) => k !== codeKey)
    .forEach((k) => {
      let score = 0;
      inputs.forEach((row) => {
        if (asNumber((row || {})[k]) !== null) score += 1;
      });
      if (score > bestScore) {
        bestScore = score;
        best = k;
      }
    });
  return best;
}

function cleanSheetRows(rows) {
  const cleaned = (rows || []).map((row) => row.map((cell) => asText(cell)));
  const nonEmpty = cleaned.filter((row) => row.some((cell) => cell !== ""));
  if (!nonEmpty.length) return [];
  const maxCols = nonEmpty.reduce((acc, row) => {
    let right = 0;
    for (let i = row.length - 1; i >= 0; i -= 1) {
      if (row[i] !== "") {
        right = i + 1;
        break;
      }
    }
    return Math.max(acc, right);
  }, 0);
  return nonEmpty.map((row) => row.slice(0, maxCols));
}

function inferStatus(score) {
  if (score >= 75) return getLang() === "zh" ? "扩张偏热" : "Expansion Overheating";
  if (score >= 60) return getLang() === "zh" ? "温和扩张" : "Moderate Expansion";
  if (score >= 45) return getLang() === "zh" ? "中性偏脆弱" : "Neutral Fragile";
  if (score >= 30) return getLang() === "zh" ? "防御区" : "Defensive";
  return getLang() === "zh" ? "衰退/危机" : "Recession/Crisis";
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function computeModelFromTables(tables) {
  const indicators = tables?.indicators || [];
  const inputs = tables?.inputs || [];
  const dimensionsTable = tables?.dimensions || [];

  const inputCodeKey = pickInputCodeKey(inputs);
  const inputValueKey = pickInputValueKey(inputs);
  const inputMap = new Map();
  inputs.forEach((row) => {
    const code = asText(row[inputCodeKey]);
    const value = asNumber(row[inputValueKey]);
    if (code && value !== null) inputMap.set(code, value);
  });

  const dimNameMap = new Map();
  const dimWeightMap = new Map();
  dimensionsTable.forEach((row) => {
    const id = asText(findValue(row, ["dimensionid", "维度id", "id"]));
    if (!id) return;
    dimNameMap.set(id, asText(findValue(row, ["dimensionname", "维度名称", "维度"])));
    dimWeightMap.set(id, asNumber(findValue(row, ["weight", "权重", "%"])) ?? 0);
  });

  const indicatorScores = [];
  indicators.forEach((row) => {
    const code = asText(findValue(row, ["indicatorcode", "code"]));
    const dim = asText(findValue(row, ["dimensionid", "维度id", "id"]));
    if (!code || !dim) return;

    const raw = inputMap.get(code);
    if (raw === undefined || raw === null) return;

    const capLow = asNumber(findValue(row, ["caplow", "低截断"]));
    const capHigh = asNumber(findValue(row, ["caphigh", "高截断"]));
    let capped = raw;
    if (capLow !== null) capped = Math.max(capped, capLow);
    if (capHigh !== null) capped = Math.min(capped, capHigh);

    const scaleType = asText(findValue(row, ["scaletype"])).toLowerCase();
    const direction = asText(findValue(row, ["direction"])).toLowerCase();
    const best = asNumber(findValue(row, ["best"]));
    const worst = asNumber(findValue(row, ["worst"]));
    const worstLow = asNumber(findValue(row, ["worstlow"]));
    const targetLow = asNumber(findValue(row, ["targetlow"]));
    const targetHigh = asNumber(findValue(row, ["targethigh"]));
    const worstHigh = asNumber(findValue(row, ["worsthigh"]));
    const weightWithinDim = asNumber(findValue(row, ["weightwithindim", "权重"])) ?? 0;

    let score = null;
    if (scaleType.includes("targetband") && worstLow !== null && targetLow !== null && targetHigh !== null && worstHigh !== null) {
      if (capped >= targetLow && capped <= targetHigh) score = 100;
      else if (capped < targetLow) score = ((capped - worstLow) / (targetLow - worstLow)) * 100;
      else score = ((worstHigh - capped) / (worstHigh - targetHigh)) * 100;
    } else if (direction.includes("higher") && best !== null && worst !== null && best !== worst) {
      score = ((capped - worst) / (best - worst)) * 100;
    } else if (direction.includes("lower") && best !== null && worst !== null && worst !== best) {
      score = ((worst - capped) / (worst - best)) * 100;
    }
    if (score === null || !Number.isFinite(score)) return;
    score = clamp(score, 0, 100);

    indicatorScores.push({
      IndicatorCode: code,
      DimensionID: dim,
      IndicatorName: asText(findValue(row, ["indicatorname", "指标", "name"])),
      LatestValue: round(raw, 4),
      CappedValue: round(capped, 4),
      "Score(0-100)": round(score, 2),
      WeightWithinDim: round(weightWithinDim, 4),
      WeightedScore: round(score * weightWithinDim, 4)
    });
  });

  const dims = [...new Set(indicatorScores.map((x) => x.DimensionID))];
  const dimensionScores = dims.map((id) => {
    const rows = indicatorScores.filter((x) => x.DimensionID === id);
    const wsum = rows.reduce((a, r) => a + (asNumber(r.WeightWithinDim) ?? 0), 0);
    const weighted = rows.reduce((a, r) => a + (asNumber(r.WeightedScore) ?? 0), 0);
    const score = wsum > 0 ? weighted / wsum : 0;
    const dimWeight = dimWeightMap.get(id) ?? 0;
    const contribution = (score * dimWeight) / 100;
    return {
      id,
      name: dimNameMap.get(id) || id,
      score,
      contribution,
      dimWeight
    };
  });

  const totalScore = dimensionScores.reduce((a, d) => a + d.contribution, 0);
  return { indicatorScores, dimensionScores, totalScore };
}

function buildDefaultAlerts(inputRows) {
  const inputCodeKey = pickInputCodeKey(inputRows);
  const inputValueKey = pickInputValueKey(inputRows);
  const valueOf = (code) => {
    const row = inputRows.find((r) => asText(r[inputCodeKey]) === code);
    return row ? asNumber(row[inputValueKey]) : null;
  };

  const checks = [
    { id: "A01", level: "RED", condition: "VIX > 30", triggered: (valueOf("VIX") ?? -Infinity) > 30 },
    { id: "A02", level: "RED", condition: "MOVE > 140", triggered: (valueOf("MOVE") ?? -Infinity) > 140 },
    { id: "A03", level: "YELLOW", condition: "HY OAS > 600bps", triggered: (valueOf("HY_OAS") ?? -Infinity) > 600 },
    { id: "A04", level: "YELLOW", condition: "10Y-3M < -50bps", triggered: (valueOf("YC_10Y3M") ?? Infinity) < -50 },
    { id: "A05", level: "YELLOW", condition: "Unemployment > 6%", triggered: (valueOf("UNRATE") ?? -Infinity) > 6 },
    { id: "A06", level: "YELLOW", condition: "Core PCE > 3.5%", triggered: (valueOf("CORE_PCE_YOY") ?? -Infinity) > 3.5 },
    { id: "A07", level: "YELLOW", condition: "WTI > 100", triggered: (valueOf("WTI") ?? -Infinity) > 100 }
  ];
  return checks;
}

function parseWorkbook(arrayBuffer) {
  const workbook = XLSX.read(arrayBuffer, { type: "array" });

  const sheetNames = {
    dimensions: findSheet(workbook, ["dimensions", "维度"]),
    indicators: findSheet(workbook, ["indicators", "指标"]),
    inputs: findSheet(workbook, ["inputs", "输入"]),
    scores: findSheet(workbook, ["scores", "分数"]),
    alerts: findSheet(workbook, ["alerts", "预警"])
  };

  const tables = {
    dimensions: sheetNames.dimensions ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.dimensions], { defval: "" }) : [],
    indicators: sheetNames.indicators ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.indicators], { defval: "" }) : [],
    inputs: sheetNames.inputs ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.inputs], { defval: "" }) : [],
    scores: sheetNames.scores ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.scores], { defval: "" }) : [],
    alerts: sheetNames.alerts ? XLSX.utils.sheet_to_json(workbook.Sheets[sheetNames.alerts], { defval: "" }) : []
  };

  const workbookSheets = (workbook.SheetNames || []).map((name) => {
    const rows = XLSX.utils.sheet_to_json(workbook.Sheets[name], { header: 1, defval: "", raw: false, blankrows: false });
    return { name, rows: cleanSheetRows(rows) };
  });

  let asOf = "";
  tables.inputs.forEach((row) => {
    if (asOf) return;
    const values = Object.values(row).map(asText);
    const dateLike = values.find((v) => /^\d{4}-\d{2}-\d{2}$/.test(v));
    if (dateLike) asOf = dateLike;
  });

  const computed = computeModelFromTables(tables);
  const dimensions = computed.dimensionScores
    .sort((a, b) => a.id.localeCompare(b.id, undefined, { numeric: true }))
    .map((d) => ({ name: d.name, score: round(d.score, 2), contribution: round(d.contribution, 4), id: d.id }));
  const totalScore = computed.totalScore || average(dimensions.map((d) => d.score));
  const alerts = buildDefaultAlerts(tables.inputs || []);

  tables.scores = computed.indicatorScores;

  const activeAlerts = alerts.filter((a) => a.triggered).length;
  const drivers = buildDrivers(dimensions, activeAlerts, totalScore);

  return normalizeModel({
    asOf: asOf || new Date().toISOString().slice(0, 10),
    totalScore: round(totalScore, 1),
    status: inferStatus(totalScore),
    alerts,
    dimensions,
    drivers,
    tables,
    workbook: { sheets: workbookSheets },
    onlineCheck: []
  });
}

function average(values) {
  if (!values.length) return sampleModel.totalScore;
  return values.reduce((acc, v) => acc + v, 0) / values.length;
}

function buildDrivers(dimensions, activeAlerts, totalScore) {
  const ranked = [...dimensions].sort((a, b) => b.score - a.score);
  const best = ranked[0];
  const worst = ranked[ranked.length - 1];
  const zh = getLang() === "zh";

  return [
    {
      title: zh ? "主要支撑" : "Primary Support",
      text: best
        ? zh
          ? `${localizeDimensionName(best.name, best.id)} 是当前最强维度（${round(best.score)}），对总分形成支撑。`
          : `${localizeDimensionName(best.name, best.id)} is the strongest block (${round(best.score)}), supporting the composite.`
        : zh
          ? "领先维度对总分形成中性支撑。"
          : "Leading dimensions are providing neutral support."
    },
    {
      title: zh ? "主要拖累" : "Primary Drag",
      text: worst
        ? zh
          ? `${localizeDimensionName(worst.name, worst.id)} 是当前主要拖累项（${round(worst.score)}）。`
          : `${localizeDimensionName(worst.name, worst.id)} remains the key drag (${round(worst.score)}).`
        : zh
          ? "弱势维度仍限制风险偏好。"
          : "Weaker dimensions still cap risk appetite."
    },
    {
      title: zh ? "风险触发" : "Risk Trigger",
      text: activeAlerts > 0
        ? zh
          ? `当前有 ${activeAlerts} 条预警触发，建议保持风控优先。`
          : `${activeAlerts} alert(s) are triggered. Keep risk controls tight.`
        : totalScore >= 60
          ? zh
            ? "当前无预警触发，策略可维持中性偏积极。"
            : "No active alerts; tactical stance can remain neutral-positive."
          : zh
            ? "当前无预警触发，但总分仍偏弱，建议均衡配置。"
            : "No active alerts, but score remains soft; keep positioning balanced."
    }
  ];
}

function pickLocalizedDrivers(inputDrivers, dimensions, activeAlerts, totalScore) {
  const list = Array.isArray(inputDrivers) ? inputDrivers : [];
  if (getLang() === "en" && list.some((d) => containsChinese(d?.title) || containsChinese(d?.text) || containsChinese(d?.summary))) {
    return buildDrivers(dimensions || [], activeAlerts || 0, totalScore || 0);
  }
  return list;
}

function objectRowsToColumns(rows) {
  const cols = new Set();
  (rows || []).forEach((row) => Object.keys(row || {}).forEach((k) => cols.add(k)));
  return [...cols];
}

function localizeColumnKey(key) {
  const k = asText(key);
  const zh = getLang() === "zh";
  const map = {
    DimensionID: zh ? "维度ID" : "DimensionID",
    DimensionName: zh ? "维度名称" : "DimensionName",
    IndicatorCode: zh ? "指标代码" : "IndicatorCode",
    IndicatorName: zh ? "指标名称" : "IndicatorName",
    Status: zh ? "状态" : "Status",
    Source: zh ? "来源" : "Source",
    LatestValue: zh ? "最新值" : "LatestValue",
    LatestDate: zh ? "最新日期" : "LatestDate",
    ValueDate: zh ? "值日期" : "ValueDate",
    Score: zh ? "评分" : "Score",
    Weight: zh ? "权重" : "Weight",
    Contribution: zh ? "贡献" : "Contribution",
    Tier: zh ? "层级" : "Tier",
    Definition: zh ? "定义" : "Definition",
    Update: zh ? "更新频率" : "Update",
    GeneratedAt: zh ? "生成时间" : "GeneratedAt"
  };
  return map[k] || k;
}

function localizeCellValue(value, key = "") {
  const text = asText(value);
  if (!text) return value;
  const keyText = asText(key).toLowerCase();
  if (keyText.includes("dimension") || keyText.includes("维度") || /^d\d{2}$/i.test(text)) return localizeDimensionName(text);
  if (keyText.includes("indicator") || keyText.includes("指标")) return localizeIndicatorName(text);
  if (keyText.includes("tier") || keyText.includes("层级")) return localizeTierName(text);
  if (keyText.includes("status") || keyText.includes("状态")) return localizeStatus(text);
  const byStatus = localizeStatus(text);
  if (byStatus !== text) return byStatus;
  const byDim = localizeDimensionName(text);
  if (byDim !== text) return byDim;
  const byInd = localizeIndicatorName(text);
  if (byInd !== text) return byInd;
  const byTier = localizeTierName(text);
  if (byTier !== text) return byTier;
  return value;
}

function renderObjectTable(targetId, rows) {
  const root = document.getElementById(targetId);
  if (!root) return;

  const rawRows = Array.isArray(rows) ? rows : [];
  const dataRows = rawRows.map((row) => {
    const mapped = {};
    Object.keys(row || {}).forEach((k) => {
      mapped[k] = localizeCellValue(row[k], k);
    });
    return mapped;
  });
  const columns = objectRowsToColumns(rawRows);
  if (!dataRows.length || !columns.length) {
    root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "暂无数据。" : "No data found."}</p>`;
    return;
  }

  const head = `<thead><tr>${columns.map((c) => `<th>${escapeHtml(localizeColumnKey(c))}</th>`).join("")}</tr></thead>`;
  const body = `<tbody>${dataRows
    .map((row) => `<tr>${columns.map((c) => `<td>${escapeHtml(row[c])}</td>`).join("")}</tr>`)
    .join("")}</tbody>`;
  root.innerHTML = `<table class="data-table">${head}${body}</table>`;
}

function renderTableLoading(targetId) {
  const root = document.getElementById(targetId);
  if (!root) return;
  root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "正在加载..." : "Loading..."}</p>`;
}

function renderSheetTable(targetId, rows) {
  const root = document.getElementById(targetId);
  if (!root) return;

  if (!rows || !rows.length) {
    root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "该工作表无数据。" : "No rows in this sheet."}</p>`;
    return;
  }

  const maxCols = rows.reduce((m, row) => Math.max(m, row.length), 0);
  const first = rows[0] || [];
  const hasHeader = first.some((c) => asText(c));
  const headers = Array.from({ length: maxCols }, (_, i) => asText(first[i]) || `Col ${i + 1}`);
  const bodyRows = hasHeader ? rows.slice(1) : rows;

  const head = `<thead><tr>${headers.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}</tr></thead>`;
  const body = `<tbody>${bodyRows
    .map(
      (row) =>
        `<tr>${Array.from({ length: maxCols }, (_, i) => `<td>${escapeHtml(row[i])}</td>`).join("")}</tr>`
    )
    .join("")}</tbody>`;

  root.innerHTML = `<table class="data-table">${head}${body}</table>`;
}

function renderWorkbookExplorer(workbook) {
  const tabs = document.getElementById("sheet-tabs");
  const table = document.getElementById("sheet-table");
  if (!tabs || !table) return;

  const sheets = workbook?.sheets || [];
  tabs.innerHTML = "";
  if (!sheets.length) {
    table.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "请加载模型文件。" : "Load model file to view all sheets."}</p>`;
    return;
  }

  let active = 0;
  const renderActive = () => {
    renderSheetTable("sheet-table", sheets[active]?.rows || []);
    tabs.querySelectorAll(".sheet-tab").forEach((button, idx) => button.classList.toggle("active", idx === active));
  };

  sheets.forEach((sheet, idx) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "sheet-tab";
    btn.textContent = `${sheet.name} (${sheet.rows.length})`;
    btn.addEventListener("click", () => {
      active = idx;
      renderActive();
    });
    tabs.appendChild(btn);
  });

  renderActive();
}

function scoreTrend(score) {
  if (score >= 60) return { cls: "trend-up", symbol: "↑" };
  if (score < 45) return { cls: "trend-down", symbol: "↓" };
  return { cls: "trend-flat", symbol: "→" };
}

function parsePercentValue(value) {
  const text = asText(value).replace("%", "");
  const n = Number(text);
  return Number.isFinite(n) ? n : 0;
}

function findDimensionMetric(model, id, name) {
  const dim = (model.dimensions || []).find((item) => {
    const n = item.name.toLowerCase();
    return (id && n.includes(id.toLowerCase())) || (name && n.includes(name.toLowerCase()));
  });
  if (dim) return dim;

  const scoreRow = (model.tables?.scores || []).find((row) => {
    const rowId = findValue(row, ["dimensionid", "维度id", "id"]).toLowerCase();
    const rowName = findValue(row, ["dimensionname", "维度名称", "维度"]).toLowerCase();
    return rowId === id.toLowerCase() || rowName === name.toLowerCase();
  });
  if (!scoreRow) return { score: 0, contribution: 0 };

  return {
    score: asNumber(findValue(scoreRow, ["dimscore", "score", "维度分"])) || 0,
    contribution: asNumber(findValue(scoreRow, ["weightedcontribution", "contribution", "贡献"])) || 0
  };
}

function renderDimensionLayers(rows, model) {
  const root = document.getElementById("dimension-layers");
  if (!root) return;

  const list = (rows || [])
    .filter((row) => /^D\d{2}$/i.test(findValue(row, ["dimensionid", "维度id", "id"])))
    .sort((a, b) => {
      const ai = findValue(a, ["dimensionid", "维度id", "id"]);
      const bi = findValue(b, ["dimensionid", "维度id", "id"]);
      return ai.localeCompare(bi, undefined, { numeric: true });
    })
    .slice(0, 14);
  if (!list.length) {
    root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "Dimensions 表暂无数据。" : "No Dimensions data found."}</p>`;
    return;
  }

  const grouped = new Map();
  list.forEach((row) => {
    const tier = findValue(row, ["tier", "层级"]) || "Other";
    if (!grouped.has(tier)) grouped.set(tier, []);
    grouped.get(tier).push(row);
  });

  root.innerHTML = "";
  const payloadIndicators = Array.isArray(model.indicatorDetails) ? model.indicatorDetails : [];
  const tableIndicators = model.tables?.indicators || [];
  const tableInputs = model.tables?.inputs || [];
  const inputCodeKey = pickInputCodeKey(tableInputs);
  const inputValKey = pickInputValueKey(tableInputs);

  for (const [tier, dims] of grouped.entries()) {
    const layer = document.createElement("section");
    layer.className = "layer-block";
    const totalWeight = dims.reduce((acc, row) => acc + parsePercentValue(findValue(row, ["weight", "权重", "%"])), 0);

    layer.innerHTML = `
      <div class="layer-header">
        <div class="layer-title">${escapeHtml(localizeTierName(tier))}</div>
        <div class="layer-weight">${getLang() === "zh" ? "层级权重" : "Layer Weight"}: ${round(totalWeight, 1)}%</div>
      </div>
      <div class="layer-dimensions"></div>
    `;

    const container = layer.querySelector(".layer-dimensions");
    dims.forEach((row) => {
      const id = findValue(row, ["dimensionid", "维度id", "id"]);
      const name = localizeDimensionName(findValue(row, ["dimensionname", "维度名称", "维度"]), id);
      const weight = findValue(row, ["weight", "权重", "%"]);
      const definition = localizeDimensionDefinition(id, findValue(row, ["definition", "说明", "定义"]));
      const update = findValue(row, ["typical update", "frequency", "更新"]);
      const metric = findDimensionMetric(model, id, name);
      const trend = scoreTrend(metric.score || 0);
      let dimIndicators = [];

      if (payloadIndicators.length) {
        dimIndicators = payloadIndicators
          .filter((x) => asText(x.DimensionID).toLowerCase() === asText(id).toLowerCase())
          .slice(0, 3)
          .map((x) => ({
            indicatorName: localizeIndicatorName(x.IndicatorName || x.IndicatorCode),
            value: x.LatestValue
          }));
      } else {
        dimIndicators = tableIndicators
          .filter((r) => findValue(r, ["dimensionid", "维度id", "id"]).toLowerCase() === id.toLowerCase())
          .slice(0, 3)
          .map((r) => {
            const code = findValue(r, ["indicatorcode", "code"]);
            const indicatorName = findValue(r, ["indicatorname", "指标", "name"]) || code;
            const input = tableInputs.find((x) => asText(x[inputCodeKey]) === code);
            const value = input ? asText(input[inputValKey]) : "";
            return { indicatorName: localizeIndicatorName(indicatorName), value };
          });
      }
      const indicatorList = dimIndicators
        .map((i) => `<li>${escapeHtml(i.indicatorName)}: ${escapeHtml(i.value ?? "--")}</li>`)
        .join("");

      const card = document.createElement("article");
      card.className = "dimension-card";
      card.innerHTML = `
        <h3>${escapeHtml(id)} ${escapeHtml(localizeDimensionName(name, id))}</h3>
        <div class="dim-row">
          <span class="dim-chip">${getLang() === "zh" ? "权重" : "Weight"}: ${escapeHtml(weight)}</span>
          <span class="dim-chip">${getLang() === "zh" ? "更新" : "Update"}: ${escapeHtml(update)}</span>
        </div>
        <div class="score-line">
          <span class="score-pill">${round(metric.score || 0, 1)}/100</span>
          <span>${getLang() === "zh" ? "贡献" : "Contribution"}: ${round(metric.contribution || 0, 2)}</span>
          <span class="${trend.cls}">${trend.symbol}</span>
        </div>
        <ul class="preview-list">${indicatorList}</ul>
        <p>${escapeHtml(definition)}</p>
      `;
      container.appendChild(card);
    });
    root.appendChild(layer);
  }
}

function renderKeyIndicators(model) {
  const root = document.getElementById("key-indicators-grid");
  if (!root) return;

  if (Array.isArray(model.keyIndicatorsSnapshot) && model.keyIndicatorsSnapshot.length) {
    root.innerHTML = "";
    model.keyIndicatorsSnapshot.slice(0, 6).forEach((item) => {
      const card = document.createElement("article");
      card.className = "indicator-mini-card";
      card.innerHTML = `
        <h3>${escapeHtml(item.title || item.label || "--")}</h3>
        <div class="indicator-value">${escapeHtml(item.value ?? "--")}</div>
        <div class="indicator-source">${escapeHtml(item.source || "")}</div>
      `;
      root.appendChild(card);
    });
    return;
  }

  const indicators = model.tables?.indicators || [];
  const inputs = model.tables?.inputs || [];
  const inputCodeKey = pickInputCodeKey(inputs);
  const inputValueKey = pickInputValueKey(inputs);

  const top = indicators.slice(0, 6).map((row) => {
    const code = findValue(row, ["indicatorcode", "code"]);
    const name = localizeIndicatorName(findValue(row, ["indicatorname", "指标", "name"]) || code);
    const source = findValue(row, ["source", "数据源", "主数据源"]);
    const input = inputs.find((x) => asText(x[inputCodeKey]) === code);
    const value = input ? asText(input[inputValueKey]) : "";
    return { code, name, source, value };
  });

  root.innerHTML = "";
  top.forEach((item) => {
    const card = document.createElement("article");
    card.className = "indicator-mini-card";
    card.innerHTML = `
      <h3>${escapeHtml(localizeIndicatorName(item.name))}</h3>
      <div class="indicator-value">${escapeHtml(item.value || "--")}</div>
      <div class="indicator-source">${escapeHtml(item.source || "")}</div>
    `;
    root.appendChild(card);
  });
}

function buildDimensionBundles(model) {
  const dims = (model.tables?.dimensions || [])
    .filter((row) => /^D\d{2}$/i.test(findValue(row, ["dimensionid", "维度id", "id"])))
    .sort((a, b) => {
      const ai = findValue(a, ["dimensionid", "维度id", "id"]);
      const bi = findValue(b, ["dimensionid", "维度id", "id"]);
      return ai.localeCompare(bi, undefined, { numeric: true });
    });

  const indicators = model.tables?.indicators || [];
  const inputs = model.tables?.inputs || [];
  const inputCodeKey = pickInputCodeKey(inputs);
  const inputValKey = pickInputValueKey(inputs);

  return dims.map((dim) => {
    const id = findValue(dim, ["dimensionid", "维度id", "id"]);
    const name = localizeDimensionName(findValue(dim, ["dimensionname", "维度名称", "维度"]), id);
    const tier = localizeTierName(findValue(dim, ["tier", "层级"]));
    const weight = findValue(dim, ["weight", "权重", "%"]);
    const metric = findDimensionMetric(model, id, name);

    const dimIndicators = indicators
      .filter((row) => findValue(row, ["dimensionid", "维度id", "id"]).toLowerCase() === id.toLowerCase())
      .slice(0, 3)
      .map((row) => {
        const code = findValue(row, ["indicatorcode", "code"]);
        const indicatorName = localizeIndicatorName(findValue(row, ["indicatorname", "指标", "name"]) || code);
        const input = inputs.find((x) => asText(x[inputCodeKey]) === code);
        const value = input ? asText(input[inputValKey]) : "";
        return { indicatorName, value };
      });

    return { id, name, tier, weight, metric, indicators: dimIndicators };
  });
}

async function renderLatestReportSummary(model) {
  const root = document.getElementById("latest-report-summary");
  const watchRoot = document.getElementById("daily-watch-items");
  if (!root) return;

  const latest = model.latestReport || (await listReports(1))[0];
  const activeAlerts = (model.alerts || []).filter((a) => a.triggered);
  const weakDims = [...(model.dimensions || [])].sort((a, b) => a.score - b.score).slice(0, 3);
  const aiInsight = model.latestReportAiInsight || model.aiInsight || {};
  const aiJson = aiInsight.insight || {};
  let aiShort = getLang() === "zh" ? asText(aiJson.short_summary_zh) : asText(aiJson.short_summary_en);
  if (!aiShort) aiShort = asText(aiInsight.short_summary || "");
  if (getLang() === "en" && containsChinese(aiShort)) aiShort = "";
  let latestSummary = asText(model.latestReportSummary || "");
  if (getLang() === "en" && containsChinese(latestSummary)) latestSummary = "";

  root.innerHTML = `
    <div class="summary-score">${round(model.totalScore, 1)}/100 · ${escapeHtml(localizeStatus(model.status))}</div>
    <div class="summary-line">${getLang() === "zh" ? "模型更新日" : "Model As-Of"}: ${escapeHtml(model.asOf)}</div>
    <div class="summary-line">${getLang() === "zh" ? "最新报告日期" : "Latest Report Date"}: ${escapeHtml(latest?.date || "--")}</div>
    <div class="summary-line">${getLang() === "zh" ? "简要结论" : "Short Summary"}: ${escapeHtml(
      aiShort ||
      latestSummary ||
        (getLang() === "zh"
          ? `当前处于${escapeHtml(localizeStatus(model.status))}，重点关注${weakDims.map((d) => localizeDimensionName(d.name, d.id)).join(" / ")}。`
          : `Current regime is ${escapeHtml(localizeStatus(model.status))}; watch ${weakDims.map((d) => localizeDimensionName(d.name, d.id)).join(" / ")}.`)
    )}</div>
  `;

  if (!watchRoot) return;
  watchRoot.innerHTML = "";
  const snapshotWatch = (model.dailyWatchedItems || model.keyWatch || []).map((x) =>
    typeof x === "string" ? x : `${x.label || x.title}: ${x.value ?? x.text ?? ""}`
  );
  const cleanSnapshotWatch = getLang() === "en" ? snapshotWatch.filter((x) => !containsChinese(x)) : snapshotWatch;
  const items = snapshotWatch.length
    ? cleanSnapshotWatch
    : [...activeAlerts.map((a) => `${a.id}: ${a.condition}`), ...weakDims.map((d) => `${localizeDimensionName(d.name, d.id)}: ${round(d.score, 1)}`)];
  if (!items.length) items.push(getLang() === "zh" ? "暂无重点关注项。" : "No urgent watch items.");

  items.slice(0, 6).forEach((text) => {
    const link = document.createElement("div");
    link.className = "report-link";
    link.textContent = text;
    watchRoot.appendChild(link);
  });
}

function getDailyAiInsightContent(report, analysis) {
  const ai = analysis || report?.aiAnalysis || report?.aiInsight || report?.reportPayload?.aiInsight || null;
  if (!ai) {
    return { shortLine: "", detailed: "", modelLabel: "--", genAt: "--" };
  }
  const insight = ai.insight || ai;
  let detailed = getLang() === "zh"
    ? asText(insight.detailed_interpretation_zh || insight.detailed_interpretation || insight.detailed_text || insight.detailed_markdown_zh)
    : asText(insight.detailed_interpretation_en || insight.detailed_interpretation || insight.detailed_markdown_en || insight.detailed_text);
  let shortLine = getLang() === "zh"
    ? asText(insight.short_summary_zh || insight.short_summary)
    : asText(insight.short_summary_en || insight.short_summary);
  if (getLang() === "en" && containsChinese(shortLine)) shortLine = "";
  if (getLang() === "en" && containsChinese(detailed)) detailed = "";
  const modelLabel = asText(ai.model || "--");
  const genAt = asText(ai.generated_at || report?.generatedAt || "--");
  return { shortLine, detailed, modelLabel, genAt };
}

function cleanAiMarkdownLine(line) {
  let s = asText(line);
  if (!s) return "";
  // Remove markdown heading markers and list markers.
  s = s.replace(/^\s*#{1,6}\s*/g, "");
  s = s.replace(/^\s*[-•]\s*/g, "");
  // Remove markdown emphasis markers.
  s = s.replace(/\*\*/g, "");
  s = s.replace(/\*/g, "");
  // Collapse extra spaces after cleanup.
  s = s.replace(/\s{2,}/g, " ").trim();
  return s;
}

function isAiSectionTitle(rawLine, cleanedLine) {
  const raw = asText(rawLine).trim();
  const cleaned = asText(cleanedLine).trim();
  if (!cleaned) return false;
  if (/^\s*#{1,6}\s+/.test(raw)) return true;
  const knownZh = ["宏观判断", "数据要点", "结构解读", "数据质量提示", "简要结论"];
  const knownEn = ["Macro view", "Key data points", "Interpretation", "Data quality note", "Short Summary"];
  return knownZh.includes(cleaned) || knownEn.includes(cleaned);
}

function parseAiInterpretationBlocks(shortLine, detailed) {
  const blocks = [];
  const shortClean = cleanAiMarkdownLine(shortLine);
  if (shortClean) {
    blocks.push({
      title: getLang() === "zh" ? "简要结论" : "Short Summary",
      items: [shortClean]
    });
  }

  const rawLines = asText(detailed).split(/\r?\n/);
  let current = null;
  rawLines.forEach((raw) => {
    const cleaned = cleanAiMarkdownLine(raw);
    if (!cleaned) return;
    const isTitle = isAiSectionTitle(raw, cleaned);
    if (isTitle) {
      current = { title: cleaned, items: [] };
      blocks.push(current);
      return;
    }
    if (!current) {
      current = { title: "", items: [] };
      blocks.push(current);
    }
    current.items.push(cleaned);
  });

  return blocks
    .map((b) => ({ title: asText(b.title), items: (b.items || []).filter(Boolean) }))
    .filter((b) => b.title || b.items.length);
}

function groupByTier(bundles) {
  const groups = new Map();
  bundles.forEach((bundle) => {
    if (!groups.has(bundle.tier)) groups.set(bundle.tier, []);
    groups.get(bundle.tier).push(bundle);
  });
  return groups;
}

function renderDailyReportPreview(model, date, report, analysis) {
  const root = document.getElementById("report-preview");
  if (!root) return;

  const topWatch = [...(model.dimensions || [])].sort((a, b) => a.score - b.score).slice(0, 5);
  const ai = getDailyAiInsightContent(report, analysis);
  const aiBlocks = parseAiInterpretationBlocks(ai.shortLine, ai.detailed);
  const showAi = aiBlocks.length > 0;

  root.innerHTML = `
    <section class="preview-header">
      <h3>${getLang() === "zh" ? `14维宏观监控模型报告 (${date})` : `14-Dimension Macro Report (${date})`}</h3>
      <div>${getLang() === "zh" ? "综合评分" : "Composite Score"}: ${round(model.totalScore, 1)}/100</div>
      <div>${getLang() === "zh" ? "投资信号" : "Signal"}: ${escapeHtml(localizeStatus(model.status))}</div>
    </section>
    <section class="preview-section">
      <h3>${getLang() === "zh" ? "核心结论" : "Core Conclusion"}</h3>
      <ul class="preview-list">
        ${topWatch.map((d) => `<li>${escapeHtml(localizeDimensionName(d.name, d.id))}: ${round(d.score, 1)}</li>`).join("")}
      </ul>
    </section>
    ${
      showAi
        ? `
    <section class="preview-section">
      <h3>${getLang() === "zh" ? "AI 报告解读" : "AI Report Interpretation"}</h3>
      ${aiBlocks
        .map(
          (block) => `
        ${block.title ? `<p class="summary-line"><strong>${escapeHtml(block.title)}</strong></p>` : ""}
        ${
          block.items.length
            ? `<ul class="preview-list">${block.items.map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>`
            : ""
        }
      `
        )
        .join("")}
    </section>`
        : ""
    }
  `;

  const list = (model.tables?.dimensions || [])
    .filter((row) => /^D\d{2}$/i.test(findValue(row, ["dimensionid", "维度id", "id"])))
    .sort((a, b) => {
      const ai = findValue(a, ["dimensionid", "维度id", "id"]);
      const bi = findValue(b, ["dimensionid", "维度id", "id"]);
      return ai.localeCompare(bi, undefined, { numeric: true });
    })
    .slice(0, 14);
  if (!list.length) return;

  const grouped = new Map();
  list.forEach((row) => {
    const tier = findValue(row, ["tier", "层级"]) || "Other";
    if (!grouped.has(tier)) grouped.set(tier, []);
    grouped.get(tier).push(row);
  });

  const payloadIndicators = Array.isArray(model.indicatorDetails) ? model.indicatorDetails : [];
  const tableIndicators = model.tables?.indicators || [];
  const tableInputs = model.tables?.inputs || [];
  const inputCodeKey = pickInputCodeKey(tableInputs);
  const inputValKey = pickInputValueKey(tableInputs);

  const section = document.createElement("section");
  section.className = "preview-section";
  section.innerHTML = `<h3>${escapeHtml(t("dimensions_14_detail"))}</h3>`;

  for (const [tier, dims] of grouped.entries()) {
    const layer = document.createElement("section");
    layer.className = "layer-block";
    const totalWeight = dims.reduce((acc, row) => acc + parsePercentValue(findValue(row, ["weight", "权重", "%"])), 0);
    layer.innerHTML = `
      <div class="layer-header">
        <div class="layer-title">${escapeHtml(localizeTierName(tier))}</div>
        <div class="layer-weight">${getLang() === "zh" ? "层级权重" : "Layer Weight"}: ${round(totalWeight, 1)}%</div>
      </div>
      <div class="layer-dimensions"></div>
    `;
    const container = layer.querySelector(".layer-dimensions");
    dims.forEach((row) => {
      const id = findValue(row, ["dimensionid", "维度id", "id"]);
      const name = findValue(row, ["dimensionname", "维度名称", "维度"]);
      const weight = findValue(row, ["weight", "权重", "%"]);
      const definition = localizeDimensionDefinition(id, findValue(row, ["definition", "说明", "定义"]));
      const update = findValue(row, ["typical update", "frequency", "更新"]);
      const metric = findDimensionMetric(model, id, name);
      const trend = scoreTrend(metric.score || 0);

      let dimIndicators = [];
      if (payloadIndicators.length) {
        dimIndicators = payloadIndicators
          .filter((x) => asText(x.DimensionID).toLowerCase() === asText(id).toLowerCase())
          .slice(0, 3)
          .map((x) => ({
            indicatorName: localizeIndicatorName(x.IndicatorName || x.IndicatorCode),
            value: x.LatestValue
          }));
      } else {
        dimIndicators = tableIndicators
          .filter((r) => findValue(r, ["dimensionid", "维度id", "id"]).toLowerCase() === id.toLowerCase())
          .slice(0, 3)
          .map((r) => {
            const code = findValue(r, ["indicatorcode", "code"]);
            const indicatorName = localizeIndicatorName(findValue(r, ["indicatorname", "指标", "name"]) || code);
            const input = tableInputs.find((x) => asText(x[inputCodeKey]) === code);
            const value = input ? asText(input[inputValKey]) : "";
            return { indicatorName, value };
          });
      }
      const indicatorList = dimIndicators.map((i) => `<li>${escapeHtml(i.indicatorName)}: ${escapeHtml(i.value ?? "--")}</li>`).join("");

      const card = document.createElement("article");
      card.className = "dimension-card";
      card.innerHTML = `
        <h3>${escapeHtml(id)} ${escapeHtml(localizeDimensionName(name, id))}</h3>
        <div class="dim-row">
          <span class="dim-chip">${getLang() === "zh" ? "权重" : "Weight"}: ${escapeHtml(weight)}</span>
          <span class="dim-chip">${getLang() === "zh" ? "更新" : "Update"}: ${escapeHtml(update)}</span>
        </div>
        <div class="score-line">
          <span class="score-pill">${round(metric.score || 0, 1)}/100</span>
          <span>${getLang() === "zh" ? "贡献" : "Contribution"}: ${round(metric.contribution || 0, 2)}</span>
          <span class="${trend.cls}">${trend.symbol}</span>
        </div>
        <ul class="preview-list">${indicatorList}</ul>
        <p>${escapeHtml(definition)}</p>
      `;
      container.appendChild(card);
    });
    section.appendChild(layer);
  }
  root.appendChild(section);
}

function findValue(row, patterns) {
  const key = Object.keys(row || {}).find((k) => patterns.some((p) => k.toLowerCase().includes(p)));
  return key ? asText(row[key]) : "";
}

function renderDashboard(model) {
  const score = document.getElementById("total-score");
  const asOf = document.getElementById("as-of");
  const status = document.getElementById("macro-status");
  const alertList = document.getElementById("alert-list");
  const bars = document.getElementById("dimension-bars");
  const drivers = document.getElementById("drivers");
  if (!score) return;

  score.textContent = round(model.totalScore, 1).toFixed(1);
  asOf.textContent = model.asOf;
  status.textContent = localizeStatus(model.status);

  const active = (model.triggerAlerts || model.alerts || []).filter((a) => a.triggered);
  alertList.innerHTML = "";
  if (!active.length) {
    const li = document.createElement("li");
    li.className = "alert-item none";
    li.textContent = getLang() === "zh" ? "当前无触发预警。" : "No active alerts.";
    alertList.appendChild(li);
  } else {
    active.forEach((alert) => {
      const li = document.createElement("li");
      li.className = `alert-item ${alert.level.toLowerCase()}`;
      li.innerHTML = `<strong>${escapeHtml(alert.id)} (${escapeHtml(alert.level)})</strong><br>${escapeHtml(alert.condition)}`;
      alertList.appendChild(li);
    });
  }

  bars.innerHTML = "";
  const topContributors = Array.isArray(model.topDimensionContributors) && model.topDimensionContributors.length
    ? model.topDimensionContributors
    : [...(model.dimensions || [])].sort((a, b) => (b.contribution || 0) - (a.contribution || 0));

  [...topContributors]
    .forEach((item) => {
      const row = document.createElement("div");
      row.className = "bar-row";
      const width = Math.max(0, Math.min(100, Number(item.score) || 0));
      row.innerHTML = `
        <div class="bar-top"><span>${escapeHtml(localizeDimensionName(item.name, item.id))}</span><span>${round(item.score, 1)}</span></div>
        <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
      `;
      bars.appendChild(row);
    });

  drivers.innerHTML = "";
  const displayDrivers = pickLocalizedDrivers(
    model.primaryDrivers || model.drivers || [],
    model.dimensions || [],
    active.length,
    model.totalScore
  );
  displayDrivers.forEach((item) => {
    const card = document.createElement("article");
    card.className = "driver-card";
    card.innerHTML = `<h3>${escapeHtml(item.title || "")}</h3><p>${escapeHtml(item.text || item.summary || "")}</p>`;
    drivers.appendChild(card);
  });

  renderKeyIndicators(model);
  renderLatestReportSummary(model);
  scheduleHeavyDashboardRender(model);
}

function renderDashboardSkeleton() {
  const bars = document.getElementById("dimension-bars");
  const drivers = document.getElementById("drivers");
  const keyGrid = document.getElementById("key-indicators-grid");
  const summary = document.getElementById("latest-report-summary");
  const watch = document.getElementById("daily-watch-items");
  if (summary) {
    summary.innerHTML = `
      <div class="skeleton-line lg"></div>
      <div class="skeleton-line"></div>
      <div class="skeleton-line"></div>
    `;
  }
  if (watch) {
    watch.innerHTML = `<div class="skeleton-line"></div><div class="skeleton-line"></div>`;
  }
  if (bars) {
    bars.innerHTML = "";
    for (let i = 0; i < 4; i += 1) {
      const row = document.createElement("div");
      row.className = "bar-row skeleton-card";
      row.innerHTML = `<div class="skeleton-line"></div><div class="skeleton-line sm"></div>`;
      bars.appendChild(row);
    }
  }
  if (drivers) {
    drivers.innerHTML = "";
    for (let i = 0; i < 3; i += 1) {
      const card = document.createElement("article");
      card.className = "driver-card skeleton-card";
      card.innerHTML = `<div class="skeleton-line"></div><div class="skeleton-line"></div>`;
      drivers.appendChild(card);
    }
  }
  if (keyGrid) {
    keyGrid.innerHTML = "";
    for (let i = 0; i < 3; i += 1) {
      const card = document.createElement("article");
      card.className = "indicator-mini-card skeleton-card";
      card.innerHTML = `<div class="skeleton-line"></div><div class="skeleton-line sm"></div>`;
      keyGrid.appendChild(card);
    }
  }
}

function scheduleHeavyDashboardRender(model) {
  dashboardHeavyRenderToken += 1;
  const token = dashboardHeavyRenderToken;
  const task = async () => {
    if (token !== dashboardHeavyRenderToken) return;
    const [dimensionsRows, inputRows, indicatorRows] = await Promise.all([
      loadDashboardSingleTable("dimensions"),
      loadDashboardSingleTable("inputs"),
      loadDashboardSingleTable("indicators")
    ]);
    if (token !== dashboardHeavyRenderToken) return;
    const merged = normalizeModel({
      ...model,
      tables: {
        ...(model.tables || {}),
        dimensions: dimensionsRows,
        inputs: inputRows,
        indicators: indicatorRows
      }
    });
    dashboardPrefetchedTableRows.indicators = indicatorRows;
    renderDimensionLayers(merged.all14DimensionsDetailed || merged.tables?.dimensions || [], merged);
    renderObjectTable("dimensions-table", merged.tables?.dimensions || []);
    renderObjectTable("inputs-table", merged.tables?.inputs || []);
    dashboardTableLoaded.dimensions = true;
    dashboardTableLoaded.inputs = true;
    renderTableLoading("indicators-table");
    renderTableLoading("scores-table");
    renderTableLoading("alerts-table");
    setupDashboardSectionTableLazyLoad("indicators", "indicators-table", token);
    setupDashboardSectionTableLazyLoad("scores", "scores-table", token);
    setupDashboardSectionTableLazyLoad("alerts", "alerts-table", token);
    scheduleDashboardPrefetchTables(token);
    setupDashboardWorkbookLazyLoad(merged, token);
  };
  if (typeof window.requestIdleCallback === "function") {
    window.requestIdleCallback(task, { timeout: 900 });
    return;
  }
  window.setTimeout(task, 60);
}

function scheduleDashboardPrefetchTables(token) {
  if (dashboardPrefetchStarted) return;
  dashboardPrefetchStarted = true;
  const run = async () => {
    const [scoresRows, alertsRows] = await Promise.all([
      loadDashboardSingleTable("scores"),
      loadDashboardSingleTable("alerts")
    ]);
    if (token !== dashboardHeavyRenderToken) return;
    dashboardPrefetchedTableRows.scores = scoresRows;
    dashboardPrefetchedTableRows.alerts = alertsRows;
  };
  if (typeof window.requestIdleCallback === "function") {
    window.requestIdleCallback(run, { timeout: 1800 });
    return;
  }
  window.setTimeout(run, 500);
}

function setupNavPrefetch() {
  const links = [...document.querySelectorAll("a.nav-link[href]")];
  const seen = new Set();
  const prefetch = (href) => {
    if (!href || seen.has(href) || href.startsWith("http")) return;
    seen.add(href);
    fetch(href, { method: "GET", cache: "force-cache" }).catch(() => {});
  };
  links.forEach((link) => {
    const href = link.getAttribute("href");
    link.addEventListener("mouseenter", () => prefetch(href), { passive: true });
    link.addEventListener("touchstart", () => prefetch(href), { passive: true });
  });
}

function setupDashboardSectionTableLazyLoad(tableName, targetId, token) {
  const target = document.getElementById(targetId);
  if (!target) return;
  const loadRows = async () => {
    if (dashboardTableLoaded[tableName]) return;
    const rows = dashboardPrefetchedTableRows[tableName] || (await loadDashboardSingleTable(tableName));
    if (token !== dashboardHeavyRenderToken) return;
    renderObjectTable(targetId, rows);
    dashboardTableLoaded[tableName] = true;
  };
  if (!("IntersectionObserver" in window)) {
    window.setTimeout(loadRows, 800);
    return;
  }
  const observer = new IntersectionObserver(
    (entries) => {
      const hit = entries.some((entry) => entry.isIntersecting);
      if (!hit) return;
      observer.disconnect();
      loadRows();
    },
    { rootMargin: "240px 0px" }
  );
  observer.observe(target);
  dashboardTableObservers.push(observer);
}

function setupDashboardWorkbookLazyLoad(model, token) {
  const tabs = document.getElementById("sheet-tabs");
  const table = document.getElementById("sheet-table");
  if (!tabs || !table) return;
  if (!dashboardWorkbookLoaded) {
    tabs.innerHTML = "";
    table.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "滚动到此区域后加载工作簿明细..." : "Workbook details load when this section enters view..."}</p>`;
  }
  const loadWorkbook = async () => {
    if (dashboardWorkbookLoaded || dashboardWorkbookLoading) return;
    dashboardWorkbookLoading = true;
    try {
      const workbook = await loadDashboardWorkbook();
      if (token !== dashboardHeavyRenderToken) return;
      const merged = { ...model, workbook };
      renderWorkbookExplorer(merged.workbook || {});
      dashboardWorkbookLoaded = true;
      if (dashboardWorkbookObserver) dashboardWorkbookObserver.disconnect();
    } finally {
      dashboardWorkbookLoading = false;
    }
  };

  if (!("IntersectionObserver" in window)) {
    window.setTimeout(loadWorkbook, 1200);
    return;
  }

  if (dashboardWorkbookObserver) dashboardWorkbookObserver.disconnect();
  dashboardWorkbookObserver = new IntersectionObserver(
    (entries) => {
      const hit = entries.some((entry) => entry.isIntersecting);
      if (hit) loadWorkbook();
    },
    { rootMargin: "300px 0px" }
  );
  dashboardWorkbookObserver.observe(table);
}

function renderDashboardSummary(summary) {
  if (!summary || summary.error) return;
  const score = document.getElementById("total-score");
  const asOf = document.getElementById("as-of");
  const status = document.getElementById("macro-status");
  const alertList = document.getElementById("alert-list");
  const bars = document.getElementById("dimension-bars");
  const drivers = document.getElementById("drivers");
  const keyGrid = document.getElementById("key-indicators-grid");
  const layers = document.getElementById("dimension-layers");
  if (score) score.textContent = round(summary.totalScore, 1).toFixed(1);
  if (asOf) asOf.textContent = summary.asOf || "--";
  if (status) status.textContent = localizeStatus(summary.status || "--");

  if (alertList) {
    const active = (summary.alerts || []).filter((a) => a.triggered);
    alertList.innerHTML = "";
    if (!active.length) {
      const li = document.createElement("li");
      li.className = "alert-item none";
      li.textContent = getLang() === "zh" ? "当前无触发预警。" : "No active alerts.";
      alertList.appendChild(li);
    } else {
      active.slice(0, 6).forEach((alert) => {
        const li = document.createElement("li");
        li.className = `alert-item ${(alert.level || "").toLowerCase()}`;
        li.innerHTML = `<strong>${escapeHtml(alert.id || "--")} (${escapeHtml(alert.level || "--")})</strong><br>${escapeHtml(alert.condition || "")}`;
        alertList.appendChild(li);
      });
    }
  }

  if (bars) {
    bars.innerHTML = "";
    (summary.topDimensionContributors || []).slice(0, 8).forEach((item) => {
      const row = document.createElement("div");
      row.className = "bar-row";
      const width = Math.max(0, Math.min(100, Number(item.score) || 0));
      row.innerHTML = `
        <div class="bar-top"><span>${escapeHtml(localizeDimensionName(item.name || "--", item.id || ""))}</span><span>${round(item.score, 1)}</span></div>
        <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
      `;
      bars.appendChild(row);
    });
  }

  if (drivers) {
    drivers.innerHTML = "";
    const displayDrivers = pickLocalizedDrivers(
      summary.primaryDrivers || [],
      summary.topDimensionContributors || [],
      (summary.alerts || []).filter((a) => a.triggered).length,
      summary.totalScore
    );
    displayDrivers.slice(0, 3).forEach((item) => {
      const card = document.createElement("article");
      card.className = "driver-card";
      card.innerHTML = `<h3>${escapeHtml(item.title || "")}</h3><p>${escapeHtml(item.text || item.summary || "")}</p>`;
      drivers.appendChild(card);
    });
  }

  if (keyGrid && Array.isArray(summary.keyIndicatorsSnapshot) && summary.keyIndicatorsSnapshot.length) {
    keyGrid.innerHTML = "";
    summary.keyIndicatorsSnapshot.slice(0, 6).forEach((item) => {
      const card = document.createElement("article");
      card.className = "indicator-mini-card";
      card.innerHTML = `
        <h3>${escapeHtml(localizeIndicatorName(item.title || item.label || "--"))}</h3>
        <div class="indicator-value">${escapeHtml(item.value ?? "--")}</div>
        <div class="indicator-source">${escapeHtml(item.source || "")}</div>
      `;
      keyGrid.appendChild(card);
    });
  }

  if (layers) {
    layers.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "正在加载14维明细..." : "Loading 14-dimension details..."}</p>`;
  }
  renderLatestReportSummary({
    ...summary,
    latestReport: summary.latestReportDate
      ? {
          date: summary.latestReportDate,
          meta: { summary: summary.latestReportSummary || "" }
        }
      : null
  });
}

function isValidEmail(email) {
  return /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(asText(email));
}

function openGithubSubscriptionFallback(email) {
  const now = new Date().toISOString();
  const title = `Subscription Request: ${email}`;
  const body = [
    "Please add this email to the daily macro report mailing list.",
    "",
    `Email: ${email}`,
    `SubmittedAt: ${now}`
  ].join("\n");
  const link = `${SUBSCRIPTION_ISSUE_URL}?labels=subscription&title=${encodeURIComponent(title)}&body=${encodeURIComponent(body)}`;
  window.open(link, "_blank", "noopener,noreferrer");
}

async function setupSubscriptionForm() {
  const form = document.getElementById("subscribe-form");
  const emailInput = document.getElementById("subscribe-email");
  const status = document.getElementById("subscribe-status");
  const count = document.getElementById("subscriber-count");
  if (!form || !emailInput) return;

  const subs = await loadStaticSubscribers();
  if (count) count.textContent = `${t("subscribe_count")}: ${subs.length}`;

  if (form.dataset.bound === "1") return;
  form.dataset.bound = "1";

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const email = asText(emailInput.value).toLowerCase();
    if (!isValidEmail(email)) {
      if (status) status.textContent = getLang() === "zh" ? "请输入有效邮箱地址。" : "Please enter a valid email address.";
      return;
    }
    const base = getApiBase();
    if (base) {
      const res = await apiFetch("/api/subscribers", { method: "POST", body: JSON.stringify({ email }) });
      if (res?.ok) {
        const list = await loadStaticSubscribers();
        if (count) count.textContent = `${t("subscribe_count")}: ${list.length}`;
        if (status) status.textContent = getLang() === "zh" ? "订阅成功，已加入邮件列表。" : "Subscribed successfully.";
        emailInput.value = "";
        return;
      }
      if (res?.error === "auth_required") {
        if (status) {
          status.textContent =
            getLang() === "zh"
              ? "请先登录后再订阅。"
              : "Please sign in first, then subscribe.";
        }
        return;
      }
    }

    openGithubSubscriptionFallback(email);
    if (status) {
      status.textContent =
        getLang() === "zh"
          ? "后端暂不可用，已跳转到 GitHub 订阅请求页，请提交后完成订阅。"
          : "Backend unavailable. Redirected to GitHub subscription request page; submit it to complete subscription.";
    }
    emailInput.value = "";
  });
}

async function setupAiDataAssistant(model) {
  const input = document.getElementById("ai-query-input");
  const submit = document.getElementById("ai-query-submit");
  const status = document.getElementById("ai-query-status");
  const output = document.getElementById("ai-query-output");
  if (!input || !submit || !output) return;
  if (submit.dataset.bound === "1") return;
  submit.dataset.bound = "1";

  submit.addEventListener("click", async () => {
    const question = asText(input.value);
    if (!question) {
      if (status) status.textContent = getLang() === "zh" ? "请输入你的问题。" : "Please enter your question.";
      return;
    }
    if (status) status.textContent = getLang() === "zh" ? "AI 正在分析..." : "AI is analyzing...";
    output.innerHTML = "";
    submit.disabled = true;
    const res = await apiFetch("/api/ai/data-query", {
      method: "POST",
      body: JSON.stringify({ question, lang: getLang(), asOf: model?.asOf || "" })
    });
    submit.disabled = false;
    if (!res?.ok) {
      const msg = res?.error || "request_failed";
      if (status) status.textContent = `${getLang() === "zh" ? "查询失败" : "Query failed"}: ${msg}`;
      return;
    }
    const meta = `${res.model || "--"} · ${getLang() === "zh" ? "更新日" : "As-Of"} ${res.asOf || "--"} · ${res.generatedAt || "--"}`;
    output.innerHTML = `
      <div class="summary-line">${escapeHtml(meta)}</div>
      <div class="summary-line">${escapeHtml(res.answer || "")}</div>
    `;
    if (status) status.textContent = getLang() === "zh" ? "查询完成。" : "Done.";
  });
}

function getReportDateFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const d = params.get("date");
  return /^\d{4}-\d{2}-\d{2}$/.test(asText(d)) ? d : new Date().toISOString().slice(0, 10);
}

function updateUrlDate(date) {
  const params = new URLSearchParams(window.location.search);
  params.set("date", date);
  const query = params.toString();
  const next = `${window.location.pathname}${query ? `?${query}` : ""}`;
  history.replaceState({}, "", next);
}

function generateDailyText(model, date, onlineSummary) {
  const top = [...(model.dimensions || [])].sort((a, b) => b.score - a.score).slice(0, 3);
  const bottom = [...(model.dimensions || [])].sort((a, b) => a.score - b.score).slice(0, 3);
  const active = (model.alerts || []).filter((a) => a.triggered);
  const zh = getLang() === "zh";

  const lines = [
    zh ? "宏观监控每日报告" : "Macro Monitoring Daily Report",
    `${zh ? "报告日期" : "Date"}: ${date}`,
    `${zh ? "模型更新日" : "Model As-Of"}: ${model.asOf}`,
    `${zh ? "综合得分" : "Composite Score"}: ${round(model.totalScore, 1)} (${localizeStatus(model.status)})`,
    "",
    zh ? "执行摘要" : "Executive Summary",
    zh
      ? `- 当前模型处于“${localizeStatus(model.status)}”状态，总分 ${round(model.totalScore, 1)}。`
      : `- Current regime: ${localizeStatus(model.status)} with total score ${round(model.totalScore, 1)}.`,
    zh
      ? `- 当前触发预警 ${active.length} 条。`
      : `- ${active.length} alert(s) are currently triggered.`
  ];

  if (onlineSummary) {
    lines.push(
      zh
        ? `- 在线数据校验：检查 ${onlineSummary.checked} 项，更新 ${onlineSummary.updated} 项，失败 ${onlineSummary.failed} 项。`
        : `- Online data check: checked ${onlineSummary.checked}, updated ${onlineSummary.updated}, failed ${onlineSummary.failed}.`
    );
  }

  lines.push("", zh ? "主要支撑维度" : "Top Supporting Dimensions");
  top.forEach((item) => lines.push(`- ${localizeDimensionName(item.name, item.id)}: ${round(item.score, 1)}`));

  lines.push("", zh ? "主要拖累维度" : "Top Dragging Dimensions");
  bottom.forEach((item) => lines.push(`- ${localizeDimensionName(item.name, item.id)}: ${round(item.score, 1)}`));

  lines.push("", zh ? "预警清单" : "Alert Watchlist");
  if (!active.length) {
    lines.push(zh ? "- 今日无触发预警。" : "- No active alerts today.");
  } else {
    active.forEach((a) => lines.push(`- ${a.id} (${a.level}): ${a.condition}`));
  }

  lines.push("", zh ? "详细指标分数请见页面下方表格。" : "See the detailed indicator score table below on this page.");
  return lines.join("\n");
}

function extractSingleSeriesCode(raw) {
  const text = asText(raw);
  if (!text) return "";
  const match = text.match(/\b[A-Z][A-Z0-9_]{1,20}\b/);
  return match ? match[0] : "";
}

async function fetchFredLatestValue(seriesCode) {
  const url = `https://fred.stlouisfed.org/graph/fredgraph.csv?id=${encodeURIComponent(seriesCode)}`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const csv = await response.text();
  const lines = csv.split(/\r?\n/).slice(1).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const [date, value] = lines[i].split(",");
    if (value && value !== ".") return { date, value: Number(value) };
  }
  throw new Error("No valid value");
}

function findInputRowByCode(inputs, code) {
  const codeKey = pickInputCodeKey(inputs);
  if (!codeKey) return null;
  return inputs.find((row) => asText(row[codeKey]) === code) || null;
}

async function runOnlineDataCheck(model) {
  const indicators = model.tables?.indicators || [];
  const inputs = model.tables?.inputs || [];
  const results = [];

  const sourceKey = indicators.length ? keyByIncludes(indicators[0], ["sourceurl", "source", "数据源"]) : null;
  const seriesKey = indicators.length ? keyByIncludes(indicators[0], ["series/code", "series", "code", "建议系列"]) : null;
  const indCodeKey = indicators.length ? keyByIncludes(indicators[0], ["indicatorcode", "code"]) : null;
  const valueKey = pickInputValueKey(inputs);
  const valueDateKey = inputs.length ? keyByIncludes(inputs[0], ["valuedate", "date", "日期"]) : null;

  let checked = 0;
  let updated = 0;
  let failed = 0;

  for (const row of indicators.slice(0, 60)) {
    const source = asText(row[sourceKey]).toLowerCase();
    const code = extractSingleSeriesCode(row[seriesKey]);
    const indicatorCode = asText(row[indCodeKey]) || code;
    if (!code) continue;

    const shouldCheck = source.includes("fred") || /^([A-Z]{2,}|[A-Z0-9_]+)$/.test(code);
    if (!shouldCheck) continue;

    checked += 1;
    try {
      const latest = await fetchFredLatestValue(code);
      const inputRow = findInputRowByCode(inputs, indicatorCode) || findInputRowByCode(inputs, code);
      let changed = false;

      if (inputRow && valueKey) {
        const oldValue = asNumber(inputRow[valueKey]);
        if (oldValue === null || Math.abs(oldValue - latest.value) > 1e-9) {
          inputRow[valueKey] = round(latest.value, 4);
          changed = true;
        }
      }
      if (inputRow && valueDateKey) inputRow[valueDateKey] = latest.date;
      if (changed) updated += 1;

      results.push({ indicator: indicatorCode, source: "FRED", series: code, status: changed ? "UPDATED" : "UNCHANGED", latestDate: latest.date, latestValue: latest.value });
    } catch (err) {
      failed += 1;
      results.push({ indicator: indicatorCode, source: "FRED", series: code, status: "FAILED", latestDate: "", latestValue: "", error: asText(err.message) });
    }
  }

  const checkedAt = new Date().toISOString();
  await dbPut("checks", { id: checkedAt, checkedAt, results });
  await apiFetch("/api/checks", {
    method: "POST",
    body: JSON.stringify({ checkedAt, summary: { checked, updated, failed }, rows: results })
  });

  return {
    checked,
    updated,
    failed,
    checkedAt,
    results,
    model: {
      ...model,
      tables: {
        ...model.tables,
        inputs
      },
      onlineCheck: results
    }
  };
}

function renderReportLinks(reports) {
  const root = document.getElementById("report-links");
  if (!root) return;

  if (!reports.length) {
    root.innerHTML = `<p class="table-empty">${getLang() === "zh" ? "暂无已保存报告。" : "No saved reports yet."}</p>`;
    return;
  }

  root.innerHTML = "";
  reports.forEach((report) => {
    const item = document.createElement("article");
    item.className = "report-item";
    const scoreLabel = getLang() === "zh" ? "综合评分" : "Score";
    const signalLabel = getLang() === "zh" ? "信号" : "Signal";
    item.innerHTML = `
      <div class="report-item-main">
        <div class="report-date">${escapeHtml(report.date)}</div>
        <div class="badge-row">
          <span class="badge">${scoreLabel}: ${escapeHtml(report.meta?.score ?? "--")}</span>
          <span class="badge">${signalLabel}: ${escapeHtml(localizeStatus(report.meta?.status ?? "--"))}</span>
        </div>
      </div>
      <a class="report-open" href="${escapeHtml(report.path || `daily-report.html?date=${encodeURIComponent(report.date)}`)}">${getLang() === "zh" ? "打开" : "Open"}</a>
    `;
    root.appendChild(item);
  });
}

function renderOnlineCheckTable(rows) {
  const zh = getLang() === "zh";
  const mapped = (rows || []).map((row) => ({
    [zh ? "指标" : "Indicator"]: localizeIndicatorName(row.indicator),
    [zh ? "来源" : "Source"]: row.source,
    [zh ? "序列" : "Series"]: row.series,
    [zh ? "状态" : "Status"]: row.status,
    [zh ? "最新日期" : "LatestDate"]: row.latestDate,
    [zh ? "最新值" : "LatestValue"]: row.latestValue,
    [zh ? "错误" : "Error"]: row.error || ""
  }));
  renderObjectTable("online-check-table", mapped);
}

function renderIndicatorVerificationTable(rows) {
  const zh = getLang() === "zh";
  const mapped = (rows || []).map((r) => ({
    [zh ? "指标代码" : "IndicatorCode"]: r.IndicatorCode,
    [zh ? "指标名称" : "IndicatorName"]: localizeIndicatorName(r.IndicatorName),
    [zh ? "维度ID" : "DimensionID"]: r.DimensionID,
    [zh ? "最新值" : "LatestValue"]: r.LatestValue,
    [zh ? "值日期" : "ValueDate"]: r.ValueDate,
    [zh ? "来源日期" : "SourceDate"]: r.SourceDate,
    [zh ? "是否在线校验" : "VerifiedOnline"]: r.VerifiedOnline ? (zh ? "是" : "YES") : (zh ? "否" : "NO"),
    [zh ? "校验状态" : "VerificationStatus"]: r.VerificationStatus,
    [zh ? "校验错误" : "VerificationError"]: r.VerificationError || "",
    [zh ? "生成时间" : "GeneratedAt"]: r.GeneratedAt || ""
  }));
  renderObjectTable("indicator-verification-table", mapped);
}

async function renderDailyReport(model) {
  const date = getReportDateFromUrl();
  updateUrlDate(date);

  const scoreEl = document.getElementById("report-score");
  const dateEl = document.getElementById("report-date");
  const statusEl = document.getElementById("report-status");
  const editor = document.getElementById("report-editor");
  const saveStatus = document.getElementById("save-status");
  const regenBtn = document.getElementById("generate-report");
  const finalBtn = document.getElementById("finalize-report");
  const saveBtn = document.getElementById("save-report");
  const downloadBtn = document.getElementById("download-report");
  const runCheckBox = document.getElementById("run-online-check");

  if (!editor) return;

  scoreEl.textContent = round(model.totalScore, 1).toFixed(1);
  dateEl.textContent = date;
  statusEl.textContent = localizeStatus(model.status);

  let existing = await loadReport(date);
  if (!existing) {
    const allReports = await listReports();
    if (allReports.length) {
      existing = allReports[0];
    }
  }
  const payload = existing?.reportPayload || {};
  const viewModel = {
    ...model,
    ...payload
  };
  const initial = existing?.text || generateDailyText(model, date, null);
  editor.value = initial;
  let analysis = await loadReportAnalysis(date);
  renderDailyReportPreview(viewModel, date, existing || { reportPayload: payload }, analysis);

  renderObjectTable("daily-scores-table", viewModel.tables?.scores || model.tables?.scores || []);
  renderOnlineCheckTable(viewModel.onlineCheck || model.onlineCheck || []);
  renderIndicatorVerificationTable(viewModel.indicatorDetails || model.indicatorDetails || []);
  const generatedAtEl = document.getElementById("data-generated-at");
  if (generatedAtEl) {
    const ga = viewModel.generatedAt || model.generatedAt;
    generatedAtEl.textContent = ga ? `${t("data_generated_at")}: ${ga}` : `${t("data_generated_at")}: --`;
  }
  renderReportLinks(await listReports());

  regenBtn?.addEventListener("click", () => {
    editor.value = generateDailyText(model, date, null);
    renderDailyReportPreview(viewModel, date, existing || { reportPayload: payload }, analysis);
    saveStatus.textContent = getLang() === "zh" ? "草稿已重新生成。" : "Draft regenerated.";
  });

  finalBtn?.addEventListener("click", async () => {
    let targetModel = model;
    let summary = null;

    if (runCheckBox?.checked) {
      saveStatus.textContent = getLang() === "zh" ? "正在执行在线数据校验..." : "Running online data check...";
      const checked = await runOnlineDataCheck(targetModel);
      targetModel = checked.model;
      summary = { checked: checked.checked, updated: checked.updated, failed: checked.failed };
      await saveCurrentModel(targetModel);
      renderOnlineCheckTable(checked.results);
      renderObjectTable("daily-scores-table", targetModel.tables?.scores || []);
      renderIndicatorVerificationTable(targetModel.indicatorDetails || []);
    }

    editor.value = generateDailyText(targetModel, date, summary);
    await saveReport(
      date,
      editor.value,
      { score: round(targetModel.totalScore, 1), status: targetModel.status },
      { reportPayload: existing?.reportPayload || targetModel, aiAnalysis: analysis || existing?.aiAnalysis || null }
    );
    existing = await loadReport(date);
    analysis = await loadReportAnalysis(date);
    renderDailyReportPreview(targetModel, date, existing || { reportPayload: targetModel }, analysis);
    renderReportLinks(await listReports());
    saveStatus.textContent = getLang() === "zh" ? "最终报告已生成并保存。" : "Final report generated and saved.";
  });

  saveBtn?.addEventListener("click", async () => {
    await saveReport(
      date,
      editor.value,
      { score: round(model.totalScore, 1), status: model.status },
      { reportPayload: existing?.reportPayload || payload || viewModel, aiAnalysis: analysis || existing?.aiAnalysis || null }
    );
    existing = await loadReport(date);
    analysis = await loadReportAnalysis(date);
    renderDailyReportPreview(viewModel, date, existing || { reportPayload: payload }, analysis);
    renderReportLinks(await listReports());
    saveStatus.textContent = getLang() === "zh" ? "已保存。" : "Saved.";
  });

  downloadBtn?.addEventListener("click", () => {
    const blob = new Blob([editor.value], { type: "text/plain;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `macro-daily-report-${date}.txt`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(link.href);
  });
}

function mapTierToCategory(tier) {
  const t = asText(tier).toLowerCase();
  if (t.includes("core")) return "core-macro";
  if (t.includes("policy") || t.includes("external") || t.includes("soft")) return "policy-external";
  if (t.includes("market") || t.includes("shock")) return "market-mapping";
  if (t.includes("theme")) return "theme-panel";
  return "core-macro";
}

function referenceGlossaryCatalog() {
  const zh = getLang() === "zh";
  return [
    {
      category: "core-macro",
      title: zh ? "D01 货币政策与流动性" : "D01 Monetary Policy & Liquidity",
      weight: "12%",
      definition: zh ? "利率曲线、短端资金利率、央行资产负债表与净流动性。" : "Yield curve, short-end funding, central bank balance sheet, and net liquidity.",
      details: zh ? "关键指标: 10Y-3M利差、SOFR、美联储总资产、净流动性代理" : "Key indicators: 10Y-3M spread, SOFR, Fed assets, net liquidity proxy",
      why: zh ? "用于判断流动性环境和政策方向是否支持风险资产。" : "Measures whether policy/liquidity backdrop supports risk assets.",
      read: zh ? "曲线修复、资金利率稳定、净流动性改善通常对应更高风险偏好。" : "Curve normalization and improving net liquidity are usually risk-supportive.",
      use: zh ? "在日报中作为市场风格切换的前置信号。" : "Use as a lead signal for market regime/style shifts."
    },
    {
      category: "core-macro",
      title: zh ? "D02 增长与前瞻" : "D02 Growth & Forward Signals",
      weight: "11%",
      definition: zh ? "GDP/PMI/订单/初请等前瞻增长信号。" : "Forward growth indicators such as GDP, PMI, orders, and claims.",
      details: zh ? "关键指标: 实际GDP、制造业PMI、初请失业金4周均值、LEI同比" : "Key indicators: real GDP, PMI, claims 4WMA, LEI YoY",
      why: zh ? "用于跟踪经济动能是否放缓或再加速。" : "Tracks macro momentum deceleration vs re-acceleration.",
      read: zh ? "前瞻项恶化往往早于盈利和就业回落。" : "Forward indicators typically lead earnings and labor turns.",
      use: zh ? "结合D05盈利维度用于判断景气持续性。" : "Pair with D05 to assess cycle sustainability."
    },
    {
      category: "core-macro",
      title: zh ? "D03 通胀与价格压力" : "D03 Inflation & Price Pressure",
      weight: "10%",
      definition: zh ? "核心通胀与通胀预期，判断政策约束与利润压力。" : "Core inflation and inflation expectations to infer policy/profit pressure.",
      details: zh ? "关键指标: 核心CPI、核心PCE、5Y5Y通胀预期" : "Key indicators: core CPI, core PCE, 5Y5Y inflation expectation",
      why: zh ? "决定利率路径、估值折现和利润空间。" : "Drives rates path, valuation discounting, and margin pressure.",
      read: zh ? "通胀回落至目标区间通常利好风险资产估值。" : "Disinflation toward target is generally supportive for valuations.",
      use: zh ? "与D01联动判断政策松紧边际变化。" : "Use with D01 to track policy tightening/easing margins."
    },
    {
      category: "core-macro",
      title: zh ? "D04 就业与居民部门" : "D04 Labor & Households",
      weight: "10%",
      definition: zh ? "就业、收入与居民信用压力。" : "Labor, income, and household credit stress.",
      details: zh ? "关键指标: 失业率、工资增速、信用卡拖欠率、实际可支配收入同比" : "Key indicators: unemployment, wage growth, card delinquency, real disposable income YoY",
      why: zh ? "居民部门是消费和经济韧性的核心。" : "Households drive consumption and macro resilience.",
      read: zh ? "失业恶化+拖欠上行通常对应防御风格上升。" : "Rising unemployment and delinquencies usually favor defensive posture.",
      use: zh ? "用于判断增长下行是否进入需求收缩阶段。" : "Helps detect transition into demand contraction."
    },
    {
      category: "core-macro",
      title: zh ? "D05 企业盈利与信用" : "D05 Earnings & Credit",
      weight: "8%",
      definition: zh ? "盈利预期修正与信用利差/违约压力。" : "Earnings revisions and credit-spread/default stress.",
      details: zh ? "关键指标: 标普500 Forward P/E、EPS修正广度、HY OAS" : "Key indicators: S&P 500 Fwd P/E, EPS revision breadth, HY OAS",
      why: zh ? "连接估值端和信用端，是资产定价核心桥梁。" : "Bridges equity valuation and credit stress in pricing.",
      read: zh ? "EPS下修与OAS走阔同时出现时风险上升更快。" : "Simultaneous EPS downgrades and OAS widening raise risk sharply.",
      use: zh ? "用于行业配置和信用敞口管理。" : "Use for sector allocation and credit exposure control."
    },
    {
      category: "core-macro",
      title: zh ? "D06 房地产与利率敏感部门" : "D06 Housing & Rate-sensitive Sectors",
      weight: "6%",
      definition: zh ? "按揭利率、成交与开工，反映利率传导。" : "Mortgage rates, transactions, and starts as rate transmission channel.",
      details: zh ? "关键指标: 30年按揭利率、成屋销售、新屋开工" : "Key indicators: 30Y mortgage, existing home sales, housing starts",
      why: zh ? "地产是政策利率传导到实体的重要通道。" : "Housing is a key channel from policy rates to real activity.",
      read: zh ? "融资成本高企+成交疲弱通常压制后续增长。" : "High financing costs + weak transactions usually weigh on growth.",
      use: zh ? "可作为衰退风险早期筛查项之一。" : "Use as an early recession-risk filter."
    },
    {
      category: "core-macro",
      title: zh ? "D10 金融条件与信用传导" : "D10 Financial Conditions & Credit Transmission",
      weight: "6%",
      definition: zh ? "金融条件指数、银行信贷供给与资金面压力。" : "FCI, bank credit supply, and funding pressure.",
      details: zh ? "关键指标: FCI、银行信贷增速、TED利差" : "Key indicators: FCI, bank lending growth, TED spread",
      why: zh ? "决定政策变化能否有效传导至实体融资端。" : "Shows whether policy changes pass through to real financing conditions.",
      read: zh ? "金融条件收紧且信贷放缓时，景气下行风险增加。" : "Tighter conditions with slower lending increase downside risk.",
      use: zh ? "用于判断信用周期的拐点位置。" : "Use to detect turning points in credit cycle."
    },
    {
      category: "policy-external",
      title: zh ? "D08 外部部门与美元条件" : "D08 External Sector & Dollar Conditions",
      weight: "7%",
      definition: zh ? "DXY/利差/资本流动等外部融资条件。" : "DXY, cross-rate spreads, and capital flows.",
      details: zh ? "关键指标: DXY/REER、美日利差、海外净买入美国资产" : "Key indicators: DXY/REER, US-JP spread, foreign net buying of US assets",
      why: zh ? "美元与跨境资金流影响全球流动性与风险偏好。" : "Dollar/flow dynamics shape global liquidity and risk appetite.",
      read: zh ? "美元过强通常对应外部融资条件收紧。" : "Overly strong USD usually tightens external financing conditions.",
      use: zh ? "用于跨市场和汇率敏感资产的风险管理。" : "Use for cross-market and FX-sensitive risk management."
    },
    {
      category: "policy-external",
      title: zh ? "D09 财政政策与债务约束" : "D09 Fiscal Policy & Debt Constraint",
      weight: "8%",
      definition: zh ? "赤字、利息负担与债务路径。" : "Deficit, interest burden, and debt trajectory.",
      details: zh ? "关键指标: 财政赤字/GDP、债务/GDP、利息支出/财政收入" : "Key indicators: deficit/GDP, debt/GDP, interest/revenue",
      why: zh ? "财政可持续性决定中长期政策空间与期限溢价。" : "Fiscal sustainability drives long-run policy room and term premium.",
      read: zh ? "利息负担上行快于收入时，财政约束显著强化。" : "Interest burden rising faster than revenue tightens fiscal constraints.",
      use: zh ? "用于评估长端利率与风险资产估值约束。" : "Use to assess long-rate and valuation constraints."
    },
    {
      category: "policy-external",
      title: zh ? "D12 信心与不确定性" : "D12 Confidence & Uncertainty",
      weight: "6%",
      definition: zh ? "消费者/企业信心与政策不确定性。" : "Consumer/business confidence and policy uncertainty.",
      details: zh ? "关键指标: 消费者信心、CEO Confidence、EPU" : "Key indicators: consumer confidence, CEO confidence, EPU",
      why: zh ? "软数据影响投资和招聘意愿，常领先硬数据拐点。" : "Soft data influences capex/hiring and often leads hard-data turns.",
      read: zh ? "信心回落与不确定性上升通常抑制企业支出。" : "Falling confidence and rising uncertainty usually suppress spending.",
      use: zh ? "用于验证增长指标是否会继续走弱。" : "Use to validate whether growth weakening may persist."
    },
    {
      category: "market-mapping",
      title: zh ? "D07 风险偏好与跨资产波动" : "D07 Risk Appetite & Cross-asset Volatility",
      weight: "6%",
      definition: zh ? "VIX/MOVE/回撤等市场风险映射。" : "Market risk mapping via VIX, MOVE, and drawdown metrics.",
      details: zh ? "关键指标: VIX、MOVE、3个月最大回撤" : "Key indicators: VIX, MOVE, 3M max drawdown",
      why: zh ? "直接反映市场风险溢价与去风险压力。" : "Directly reflects risk premia and de-risk pressure.",
      read: zh ? "波动率快速抬升通常先于资金面紧张扩散。" : "Vol spikes often precede broader stress propagation.",
      use: zh ? "用于仓位、杠杆与对冲强度管理。" : "Use for position sizing, leverage, and hedge intensity."
    },
    {
      category: "market-mapping",
      title: zh ? "D11 大宗商品与能源/地缘风险" : "D11 Commodities & Geopolitical Risk",
      weight: "5%",
      definition: zh ? "油价/商品与地缘风险对通胀和增长的冲击。" : "Commodity and geopolitical shocks to inflation and growth.",
      details: zh ? "关键指标: WTI、CRB同比、GPR指数" : "Key indicators: WTI, CRB YoY, GPR index",
      why: zh ? "外生冲击会同时改变通胀路径与增长预期。" : "Exogenous shocks can alter both inflation path and growth outlook.",
      read: zh ? "能源上行+地缘紧张上升时需防范滞胀尾部。" : "Energy spikes plus geopolitical stress raise stagflation tail risks.",
      use: zh ? "用于事件驱动风险预案与对冲。" : "Use for event-driven contingency and hedging."
    },
    {
      category: "theme-panel",
      title: zh ? "D13 AI资本开支周期（主题）" : "D13 AI Capex Cycle (Theme)",
      weight: "4%",
      definition: zh ? "云与AI资本开支/营收动能（主题观察）。" : "Cloud/AI capex and revenue momentum as theme monitor.",
      details: zh ? "关键指标: 云业务增速、AI Capex指引、半导体景气代理" : "Key indicators: cloud growth, AI capex guidance, semiconductor proxy",
      why: zh ? "反映技术投资景气和相关产业链盈利弹性。" : "Captures innovation-cycle strength and chain-level earnings beta.",
      read: zh ? "景气上行有利成长风格，但需警惕估值过热。" : "Upswing supports growth style but may raise valuation overheating risk.",
      use: zh ? "建议作为主题面板，不替代核心宏观维度。" : "Use as a theme panel, not a substitute for core macro blocks."
    },
    {
      category: "theme-panel",
      title: zh ? "D14 加密与稳定币流动性（主题）" : "D14 Crypto & Stablecoin Liquidity (Theme)",
      weight: "1%",
      definition: zh ? "稳定币与链上流动性的低权重主题观察。" : "Low-weight thematic monitor of stablecoin and on-chain liquidity.",
      details: zh ? "关键指标: BTC、USDC市值、稳定币总市值" : "Key indicators: BTC, USDC market cap, total stablecoin cap",
      why: zh ? "可提供风险偏好边际变化的补充信号。" : "Provides supplementary signal on marginal risk appetite shifts.",
      read: zh ? "波动高、噪音大，需与主模型交叉验证。" : "High volatility/noise requires cross-check with core model.",
      use: zh ? "保持低权重，避免对总分造成过度扰动。" : "Keep low weight to avoid over-influencing headline score."
    },
    {
      category: "data-source",
      title: "FRED (Federal Reserve Economic Data)",
      weight: "",
      definition: zh ? "美联储圣路易斯分行经济数据平台。" : "Federal Reserve Bank of St. Louis economic data platform.",
      details: zh ? "覆盖利率、就业、通胀、信用等核心时间序列。访问: https://fred.stlouisfed.org/" : "Covers rates, labor, inflation, credit and more. URL: https://fred.stlouisfed.org/",
      why: zh ? "主源稳定、可回溯，适合模型长期维护。" : "Stable and backtestable primary source for long-horizon maintenance.",
      read: zh ? "同一指标建议固定代码，减少口径漂移。" : "Keep fixed series code per indicator to reduce methodology drift.",
      use: zh ? "报告发布前核对最近观测与更新时间戳。" : "Validate latest observations and timestamps before publishing."
    },
    {
      category: "data-source",
      title: "BEA (Bureau of Economic Analysis)",
      weight: "",
      definition: zh ? "美国经济分析局，发布GDP/PCE等官方数据。" : "US Bureau of Economic Analysis, publisher of GDP/PCE data.",
      details: zh ? "关键数据: 实际GDP、个人消费支出、收入相关序列。访问: https://www.bea.gov/" : "Key data: real GDP, PCE, income-related series. URL: https://www.bea.gov/",
      why: zh ? "是增长与通胀核心数据的官方来源之一。" : "Official source for critical growth/inflation components.",
      read: zh ? "注意首发值与修订值版本差异。" : "Track first release vs revised vintages.",
      use: zh ? "模型中应统一使用同一版次口径。" : "Use consistent vintage methodology in the model."
    },
    {
      category: "data-source",
      title: "BLS (Bureau of Labor Statistics)",
      weight: "",
      definition: zh ? "美国劳工统计局，发布就业与CPI数据。" : "US Bureau of Labor Statistics for labor and CPI data.",
      details: zh ? "关键数据: CPI、失业率、工资等。访问: https://www.bls.gov/" : "Key data: CPI, unemployment, wages. URL: https://www.bls.gov/",
      why: zh ? "就业与通胀是政策路径判断核心变量。" : "Labor and inflation are core policy-path variables.",
      read: zh ? "应结合趋势而非单次读数判断拐点。" : "Infer turning points from trend, not one-off prints.",
      use: zh ? "与BEA/FRED交叉验证后用于日报结论。" : "Cross-check with BEA/FRED before daily conclusions."
    }
  ];
}

function referenceSignalGuide() {
  const zh = getLang() === "zh";
  return [
    {
      title: zh ? "≥70分 - 强烈看多" : ">=70 - Strong Bullish",
      text: zh ? "经济环境强劲、流动性支持明显，适合积极风险配置。" : "Strong macro/liquidity backdrop supports aggressive risk allocation."
    },
    {
      title: zh ? "60-69分 - 温和看多" : "60-69 - Mild Bullish",
      text: zh ? "基本面稳健但有不确定性，适合平衡偏进攻配置。" : "Fundamentals are healthy but mixed; balanced pro-risk stance."
    },
    {
      title: zh ? "40-59分 - 中性" : "40-59 - Neutral",
      text: zh ? "多空因素交织，建议精选资产并重视风控。" : "Mixed forces; selective positioning with tighter risk controls."
    },
    {
      title: zh ? "30-39分 - 温和看空" : "30-39 - Mild Bearish",
      text: zh ? "下行压力增加，宜降低β并提高防御仓位。" : "Downside pressure rises; reduce beta and increase defensives."
    },
    {
      title: zh ? "≤29分 - 强烈看空" : "<=29 - Strong Bearish",
      text: zh ? "衰退/危机风险高，优先资本保护与流动性管理。" : "High recession/crisis risk; prioritize capital preservation and liquidity."
    }
  ];
}

function buildGlossaryEntries(model) {
  const reference = referenceGlossaryCatalog();
  const dims = (model.tables?.dimensions || [])
    .filter((row) => /^D\\d{2}$/i.test(findValue(row, ["dimensionid", "维度id", "id"])))
    .slice(0, 14)
    .map((row) => {
      const id = findValue(row, ["dimensionid", "维度id", "id"]);
      const name = findValue(row, ["dimensionname", "维度名称", "维度"]);
      const weight = findValue(row, ["weight", "权重", "%"]);
      const tier = findValue(row, ["tier", "层级"]);
      const definition = localizeDimensionDefinition(id, findValue(row, ["definition", "定义", "说明"]));
      const update = findValue(row, ["typical update", "frequency", "更新"]);
      const indicators = (model.tables?.indicators || [])
        .filter((i) => findValue(i, ["dimensionid", "维度id", "id"]).toLowerCase() === id.toLowerCase())
        .slice(0, 3)
        .map((i) => localizeIndicatorName(findValue(i, ["indicatorname", "指标", "name"])))
        .filter(Boolean)
        .join(" / ");

      return {
        category: mapTierToCategory(tier),
        title: `${id} ${name}`,
        weight,
        definition,
        details: `${getLang() === "zh" ? "关键指标" : "Key Indicators"}: ${indicators || "--"} · ${
          getLang() === "zh" ? "更新频率" : "Update"
        }: ${update || "--"}`,
        why:
          getLang() === "zh"
            ? `该维度用于衡量“${name}”对宏观周期与风险偏好的传导影响。`
            : `This dimension measures how "${name}" transmits into macro cycle and risk appetite.`,
        read:
          getLang() === "zh"
            ? "一般而言，维度分上升代表该块环境改善；分数下降代表该块风险积累。"
            : "In general, rising dimension score means improving conditions, while falling score means risk build-up.",
        use:
          getLang() === "zh"
            ? "可与加权贡献一起观察，用于解释总分变化与日报结论。"
            : "Use with weighted contribution to explain headline score changes and daily conclusions."
      };
    });

  const sources = [...new Set((model.tables?.indicators || []).map((row) => findValue(row, ["主数据源", "source"])).filter(Boolean))]
    .slice(0, 6)
    .map((source) => ({
      category: "data-source",
      title: source,
      weight: "",
      definition: getLang() === "zh" ? "模型使用的主要数据来源。" : "Primary data source used by the model.",
      details: source,
      why:
        getLang() === "zh"
          ? "稳定、可回溯的数据源可减少模型口径漂移。"
          : "Stable and backtestable sources reduce methodology drift over time.",
      read:
        getLang() === "zh"
          ? "同一指标建议固定主源，必要时再使用备选源。"
          : "Keep a fixed primary source per indicator and use fallback sources only when needed.",
      use:
        getLang() === "zh"
          ? "发布报告前应核对数据时间戳与最近更新时间。"
          : "Validate source timestamps before publishing reports."
    }));

  return [...reference, ...dims, ...sources];
}

function renderGlossary(model) {
  const root = document.getElementById("glossary-grid");
  const search = document.getElementById("glossary-search");
  const filter = document.getElementById("glossary-filter");
  if (!root) return;

  const staticEntries = glossaryTerms.map((item) => ({
    category: "core-macro",
    title: item[getLang()].term,
    weight: "",
    definition: item[getLang()].desc,
    details: "",
    why: item[getLang()].why,
    read: item[getLang()].read,
    use: item[getLang()].use
  }));
  const entries = [...buildGlossaryEntries(model), ...staticEntries];

  const draw = () => {
    const q = asText(search?.value).toLowerCase();
    const c = asText(filter?.value) || "all";
    const filtered = entries.filter((entry) => {
      const hitCategory = c === "all" || entry.category === c;
      const blob = `${entry.title} ${entry.definition} ${entry.details} ${entry.why || ""} ${entry.read || ""} ${entry.use || ""}`.toLowerCase();
      return hitCategory && (!q || blob.includes(q));
    });

    root.innerHTML = "";
    filtered.forEach((item) => {
      const card = document.createElement("article");
      card.className = "glossary-card";
      card.innerHTML = `
        <h3>${escapeHtml(item.title)}</h3>
        ${item.weight ? `<div class="term-weight">${getLang() === "zh" ? "权重" : "Weight"}: ${escapeHtml(item.weight)}</div>` : ""}
        <p><strong>${getLang() === "zh" ? "定义" : "Definition"}:</strong> ${escapeHtml(item.definition)}</p>
        ${item.details ? `<p>${escapeHtml(item.details)}</p>` : ""}
        ${item.why ? `<p><strong>${getLang() === "zh" ? "为什么重要" : "Why It Matters"}:</strong> ${escapeHtml(item.why)}</p>` : ""}
        ${item.read ? `<p><strong>${getLang() === "zh" ? "如何解读" : "How To Read"}:</strong> ${escapeHtml(item.read)}</p>` : ""}
        ${item.use ? `<p><strong>${getLang() === "zh" ? "实务使用" : "Practical Use"}:</strong> ${escapeHtml(item.use)}</p>` : ""}
      `;
      root.appendChild(card);
    });
  };

  search?.addEventListener("input", draw);
  filter?.addEventListener("change", draw);
  draw();

  const signalRoot = document.getElementById("signal-guide-grid");
  if (signalRoot) {
    signalRoot.innerHTML = "";
    referenceSignalGuide().forEach((s) => {
      const card = document.createElement("article");
      card.className = "signal-card";
      card.innerHTML = `<h3>${escapeHtml(s.title)}</h3><p>${escapeHtml(s.text)}</p>`;
      signalRoot.appendChild(card);
    });
  }
}

function renderIndicatorsPage(model) {
  const rows = model.tables?.indicators || [];
  const zh = getLang() === "zh";
  const mapped = rows.map((row) => {
    const code = findValue(row, ["indicatorcode", "code"]);
    const name = localizeIndicatorName(findValue(row, ["indicatorname", "指标", "name"]) || code);
    const dimId = findValue(row, ["dimensionid", "维度id", "id"]);
    const dimName = localizeDimensionName(findValue(row, ["dimensionname", "维度名称", "维度"]), dimId);
    const source = findValue(row, ["source", "数据源", "主数据源"]);
    const update = findValue(row, ["typical update", "frequency", "更新"]);
    const direction = findValue(row, ["direction", "方向"]);
    return zh
      ? {
          指标代码: code,
          指标名称: name,
          维度: `${dimId} ${dimName}`.trim(),
          数据源: source,
          更新频率: update,
          方向: direction
        }
      : {
          IndicatorCode: code,
          IndicatorName: name,
          Dimension: `${dimId} ${dimName}`.trim(),
          Source: source,
          Update: update,
          Direction: direction
        };
  });
  renderObjectTable("indicators-page-table", mapped);
}

async function renderOpenRouterPage() {
  renderTableLoading("openrouter-models");
  renderTableLoading("openrouter-apps");
  renderTableLoading("openrouter-providers");
  renderTableLoading("openrouter-prompts");
  const updated = document.getElementById("openrouter-updated");
  const source = document.getElementById("openrouter-source");
  const viewSel = document.getElementById("openrouter-view");
  const catSel = document.getElementById("openrouter-category");
  const refreshBtn = document.getElementById("openrouter-refresh");

  const view = asText(viewSel?.value) || "week";
  const category = asText(catSel?.value) || "all";
  const query = `?view=${encodeURIComponent(view)}&category=${encodeURIComponent(category)}`;

  const payload = await apiFetch(`/api/openrouter/rankings${query}`);
  if (!payload?.ok) {
    const msg = getLang() === "zh" ? "暂时无法获取 OpenRouter 排行数据。" : "Unable to fetch OpenRouter rankings for now.";
    renderObjectTable("openrouter-models", []);
    renderObjectTable("openrouter-apps", []);
    renderObjectTable("openrouter-providers", []);
    renderObjectTable("openrouter-prompts", []);
    const m = document.getElementById("openrouter-models");
    const a = document.getElementById("openrouter-apps");
    const p = document.getElementById("openrouter-providers");
    const pr = document.getElementById("openrouter-prompts");
    if (m) m.innerHTML = `<p class="table-empty">${escapeHtml(msg)}</p>`;
    if (a) a.innerHTML = `<p class="table-empty">${escapeHtml(msg)}</p>`;
    if (p) p.innerHTML = `<p class="table-empty">${escapeHtml(msg)}</p>`;
    if (pr) pr.innerHTML = `<p class="table-empty">${escapeHtml(msg)}</p>`;
    return;
  }

  if (updated) updated.textContent = `${t("openrouter_updated")}: ${asText(payload.fetchedAt) || "--"}`;
  if (source) {
    const url = asText(payload.sourceUrl) || "https://openrouter.ai/rankings";
    source.innerHTML = `${escapeHtml(t("openrouter_source"))}: <a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(url)}</a>`;
  }

  const zh = getLang() === "zh";
  const modelRows = (payload.models || []).map((x) =>
    zh
      ? {
          排名: x.rank,
          模型: x.name,
          提供方: x.creator || "--",
          用量: x.tokens || "--",
          占比: x.share || "--"
        }
      : {
          Rank: x.rank,
          Model: x.name,
          Provider: x.creator || "--",
          Tokens: x.tokens || "--",
          Share: x.share || "--"
        }
  );
  const appRows = (payload.apps || []).map((x) =>
    zh
      ? {
          排名: x.rank,
          应用: x.name,
          发布方: x.creator || "--",
          用量: x.tokens || "--"
        }
      : {
          Rank: x.rank,
          App: x.name,
          Creator: x.creator || "--",
          Tokens: x.tokens || "--"
        }
  );
  const providerRows = (payload.providers || []).map((x) =>
    zh
      ? {
          排名: x.rank,
          提供方: x.name,
          负责人: x.creator || "--",
          用量: x.tokens || "--",
          占比: x.share || "--"
        }
      : {
          Rank: x.rank,
          Provider: x.name,
          Owner: x.creator || "--",
          Tokens: x.tokens || "--",
          Share: x.share || "--"
        }
  );
  const promptRows = (payload.prompts || []).map((x) =>
    zh
      ? {
          排名: x.rank,
          提示词: x.name,
          作者: x.creator || "--",
          用量: x.tokens || "--",
          占比: x.share || "--"
        }
      : {
          Rank: x.rank,
          Prompt: x.name,
          Author: x.creator || "--",
          Tokens: x.tokens || "--",
          Share: x.share || "--"
        }
  );

  renderObjectTable("openrouter-models", modelRows);
  renderObjectTable("openrouter-apps", appRows);
  renderObjectTable("openrouter-providers", providerRows);
  renderObjectTable("openrouter-prompts", promptRows);

  if (!openrouterControlsBound) {
    openrouterControlsBound = true;
    viewSel?.addEventListener("change", () => {
      renderOpenRouterPage();
    });
    catSel?.addEventListener("change", () => {
      renderOpenRouterPage();
    });
    refreshBtn?.addEventListener("click", () => {
      renderOpenRouterPage();
    });
  }
}

async function loadDefaultWorkbook() {
  const response = await fetch(DEFAULT_MODEL_FILE, { cache: "no-cache" });
  if (!response.ok) throw new Error(`Unable to fetch ${DEFAULT_MODEL_FILE}`);
  return response.arrayBuffer();
}

function setupUpload(onLoaded) {
  const input = document.getElementById("xlsx-input");
  const status = document.getElementById("file-status");
  if (!input) return;

  input.addEventListener("change", async (event) => {
    const [file] = event.target.files || [];
    if (!file) return;

    try {
      const buffer = await file.arrayBuffer();
      const model = parseWorkbook(buffer);
      await saveCurrentModel(model);
      if (status) status.textContent = getLang() === "zh" ? `已加载：${file.name}` : `Loaded: ${file.name}`;
      onLoaded(model);
    } catch {
      if (status) status.textContent = getLang() === "zh" ? "文件解析失败，请重试。" : "Failed to parse file.";
    }
  });
}

function ensureSiteFooter() {
  if (document.querySelector(".site-footer-note")) return;
  const footer = document.createElement("footer");
  footer.className = "site-footer-note";
  footer.textContent = "由 Nexo Marco Intelligence 提供支持";
  document.body.appendChild(footer);
}

async function initDashboard() {
  const status = document.getElementById("file-status");
  renderDashboardSkeleton();
  dashboardWorkbookLoaded = false;
  dashboardWorkbookLoading = false;
  dashboardPrefetchStarted = false;
  dashboardTableObservers.forEach((o) => o.disconnect());
  dashboardTableObservers = [];
  Object.keys(dashboardTableLoaded).forEach((k) => {
    dashboardTableLoaded[k] = false;
  });
  Object.keys(dashboardPrefetchedTableRows).forEach((k) => {
    delete dashboardPrefetchedTableRows[k];
  });
  if (dashboardWorkbookObserver) {
    dashboardWorkbookObserver.disconnect();
    dashboardWorkbookObserver = null;
  }
  const summary = await loadDashboardSummary();
  if (summary) renderDashboardSummary(summary);
  const coreModel = await loadCurrentModel({ view: "core" });
  if (!summary) renderDashboardSummary(coreModel);
  renderDashboard(coreModel);
  if (status) status.textContent = getLang() === "zh" ? "核心数据已加载，正在补全明细..." : "Core data loaded, hydrating details...";

  setupUpload((next) => {
    dashboardWorkbookLoaded = false;
    dashboardWorkbookLoading = false;
    dashboardTableObservers.forEach((o) => o.disconnect());
    dashboardTableObservers = [];
    Object.keys(dashboardTableLoaded).forEach((k) => {
      dashboardTableLoaded[k] = false;
    });
    dashboardPrefetchStarted = false;
    Object.keys(dashboardPrefetchedTableRows).forEach((k) => {
      delete dashboardPrefetchedTableRows[k];
    });
    if (dashboardWorkbookObserver) {
      dashboardWorkbookObserver.disconnect();
      dashboardWorkbookObserver = null;
    }
    renderDashboard(next);
  });
  await setupSubscriptionForm();
}

async function initAiAssistantPage() {
  const model = await loadCurrentModel({ view: "core" });
  await setupAiDataAssistant(model);
}

async function ensureModelData(model) {
  const hasData = (model?.tables?.dimensions || []).length || (model?.tables?.indicators || []).length;
  if (hasData) return model;

  try {
    const buffer = await loadDefaultWorkbook();
    const parsed = parseWorkbook(buffer);
    await saveCurrentModel(parsed);
    return parsed;
  } catch {
    return model;
  }
}

async function init() {
  trackPublicPageView();
  setLang(getLang());
  applyI18n();
  ensureSiteFooter();
  setupNavPrefetch();
  const page = document.body.dataset.page;

  setupLangToggle(async () => {
    if (page === "polymarket" && typeof window.applyPolymarketI18n === "function") {
      window.applyPolymarketI18n();
      return;
    }
    if (page === "polymarket-pnl" && typeof window.applyPolymarketPnlI18n === "function") {
      await window.applyPolymarketPnlI18n();
      return;
    }
    if (page === "about") return;

    const currentModel = await loadCurrentModel();
    if (page === "dashboard") {
      renderDashboard(currentModel);
      await setupSubscriptionForm();
    }
    if (page === "daily-report") renderDailyReport(currentModel);
    if (page === "indicators") renderIndicatorsPage(currentModel);
    if (page === "glossary") renderGlossary(currentModel);
    if (page === "subscribe") await setupSubscriptionForm();
    if (page === "ai-assistant") await initAiAssistantPage();
    if (page === "openrouter") await renderOpenRouterPage();
  });

  // Static/special pages should not depend on model/db boot sequence.
  // But page-local i18n renderers must run once at first load.
  if (page === "about") return;
  if (page === "polymarket") {
    if (typeof window.applyPolymarketI18n === "function") window.applyPolymarketI18n();
    return;
  }
  if (page === "polymarket-pnl") {
    if (typeof window.applyPolymarketPnlI18n === "function") await window.applyPolymarketPnlI18n();
    return;
  }

  await migrateBrowserDataToServer();
  const model = await ensureModelData(await loadCurrentModel());

  if (page === "dashboard") await initDashboard();
  if (page === "daily-report") await renderDailyReport(model);
  if (page === "indicators") renderIndicatorsPage(model);
  if (page === "glossary") renderGlossary(model);
  if (page === "subscribe") await setupSubscriptionForm();
  if (page === "ai-assistant") await initAiAssistantPage();
  if (page === "openrouter") await renderOpenRouterPage();
}

document.addEventListener("DOMContentLoaded", init);
