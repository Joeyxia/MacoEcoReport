#!/usr/bin/env python3
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = Path(os.environ.get("MACRO_DB_PATH", str(DATA_DIR / "macro_monitor.db")))


def now_iso():
  return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def get_conn():
  DB_PATH.parent.mkdir(parents=True, exist_ok=True)
  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  return conn


def init_db():
  conn = get_conn()
  conn.executescript(
    """
    CREATE TABLE IF NOT EXISTS model_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      as_of TEXT,
      report_date TEXT,
      generated_at TEXT,
      payload_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sheet_rows (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sheet_name TEXT NOT NULL,
      row_index INTEGER NOT NULL,
      row_json TEXT NOT NULL,
      as_of TEXT,
      created_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_sheet_rows_unique ON sheet_rows(sheet_name, row_index, as_of);

    CREATE TABLE IF NOT EXISTS daily_reports (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      report_date TEXT UNIQUE NOT NULL,
      score REAL,
      status TEXT,
      summary TEXT,
      ai_short_summary TEXT,
      ai_detailed_interpretation TEXT,
      ai_short_summary_zh TEXT,
      ai_short_summary_en TEXT,
      ai_detailed_interpretation_zh TEXT,
      ai_detailed_interpretation_en TEXT,
      ai_model TEXT,
      ai_status TEXT,
      ai_generated_at TEXT,
      report_text TEXT,
      report_path TEXT,
      payload_json TEXT,
      generated_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS online_checks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      checked_at TEXT NOT NULL,
      summary_json TEXT,
      rows_json TEXT
    );

    CREATE TABLE IF NOT EXISTS subscribers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      source TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS user_accounts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      password_salt TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      invited_by TEXT,
      invite_code TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_login_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_user_accounts_email ON user_accounts(email);

    CREATE TABLE IF NOT EXISTS invite_codes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      max_uses INTEGER NOT NULL DEFAULT 1,
      used_count INTEGER NOT NULL DEFAULT 0,
      expires_at TEXT,
      note TEXT,
      created_by TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_invite_codes_status ON invite_codes(status, expires_at);

    CREATE TABLE IF NOT EXISTS invite_redemptions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT NOT NULL,
      email TEXT NOT NULL,
      redeemed_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_invite_redemptions_code ON invite_redemptions(code, redeemed_at DESC);

    CREATE TABLE IF NOT EXISTS email_dispatch_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      report_date TEXT,
      generated_at TEXT,
      sent INTEGER DEFAULT 0,
      failed INTEGER DEFAULT 0,
      recipients INTEGER DEFAULT 0,
      payload_json TEXT
    );

    CREATE TABLE IF NOT EXISTS email_event_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL,
      event_type TEXT NOT NULL,
      payload_json TEXT,
      created_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_email_event_unique ON email_event_logs(email, event_type);

    CREATE TABLE IF NOT EXISTS email_delivery_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL,
      report_date TEXT,
      email_type TEXT NOT NULL,
      status TEXT NOT NULL,
      detail TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_email_delivery_unique ON email_delivery_logs(email, report_date, email_type);

    CREATE TABLE IF NOT EXISTS monitor_page_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      path TEXT,
      referrer TEXT,
      user_agent TEXT,
      ip TEXT,
      visited_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_monitor_page_events_time ON monitor_page_events(visited_at);
    CREATE INDEX IF NOT EXISTS idx_monitor_page_events_path ON monitor_page_events(path);

    CREATE TABLE IF NOT EXISTS monitor_token_usage (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT,
      model TEXT,
      input_tokens INTEGER DEFAULT 0,
      output_tokens INTEGER DEFAULT 0,
      total_tokens INTEGER DEFAULT 0,
      meta_json TEXT,
      logged_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_monitor_token_usage_time ON monitor_token_usage(logged_at);

    CREATE TABLE IF NOT EXISTS api_credentials (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      service TEXT UNIQUE NOT NULL,
      api_key TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS daily_report_ai_insights (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      report_date TEXT UNIQUE NOT NULL,
      short_summary TEXT,
      detailed_text TEXT,
      insight_json TEXT,
      status TEXT,
      model TEXT,
      prompt_version TEXT,
      generated_at TEXT,
      error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_daily_report_ai_insights_date ON daily_report_ai_insights(report_date);

    CREATE TABLE IF NOT EXISTS openrouter_fetch_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      view TEXT NOT NULL,
      category TEXT NOT NULL,
      source_url TEXT,
      parse_mode TEXT,
      fetched_at TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_fetch_runs_vc ON openrouter_fetch_runs(view, category, created_at DESC);

    CREATE TABLE IF NOT EXISTS openrouter_top_models (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetch_run_id INTEGER NOT NULL,
      rank_num INTEGER,
      name TEXT,
      creator TEXT,
      tokens TEXT,
      share TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(fetch_run_id) REFERENCES openrouter_fetch_runs(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_top_models_run ON openrouter_top_models(fetch_run_id, rank_num);

    CREATE TABLE IF NOT EXISTS openrouter_top_apps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetch_run_id INTEGER NOT NULL,
      rank_num INTEGER,
      name TEXT,
      creator TEXT,
      tokens TEXT,
      share TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(fetch_run_id) REFERENCES openrouter_fetch_runs(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_top_apps_run ON openrouter_top_apps(fetch_run_id, rank_num);

    CREATE TABLE IF NOT EXISTS openrouter_top_providers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetch_run_id INTEGER NOT NULL,
      rank_num INTEGER,
      name TEXT,
      creator TEXT,
      tokens TEXT,
      share TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(fetch_run_id) REFERENCES openrouter_fetch_runs(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_top_providers_run ON openrouter_top_providers(fetch_run_id, rank_num);

    CREATE TABLE IF NOT EXISTS openrouter_top_prompts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetch_run_id INTEGER NOT NULL,
      rank_num INTEGER,
      name TEXT,
      creator TEXT,
      tokens TEXT,
      share TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(fetch_run_id) REFERENCES openrouter_fetch_runs(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_openrouter_top_prompts_run ON openrouter_top_prompts(fetch_run_id, rank_num);

    CREATE TABLE IF NOT EXISTS ticker_profiles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL UNIQUE,
      company_name TEXT,
      exchange TEXT,
      sector TEXT,
      industry TEXT,
      currency TEXT,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS stock_prices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      trade_date TEXT NOT NULL,
      open REAL,
      high REAL,
      low REAL,
      close REAL,
      adj_close REAL,
      volume REAL,
      source_file TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_prices_unique ON stock_prices(ticker, trade_date);
    CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date ON stock_prices(ticker, trade_date DESC);

    CREATE TABLE IF NOT EXISTS stock_valuations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      valuation_date TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      metric_value REAL,
      source_file TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_valuations_unique ON stock_valuations(ticker, valuation_date, metric_name);
    CREATE INDEX IF NOT EXISTS idx_stock_valuations_ticker_date ON stock_valuations(ticker, valuation_date DESC);

    CREATE TABLE IF NOT EXISTS stock_financials (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      report_date TEXT NOT NULL,
      statement_type TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      metric_value REAL,
      source_file TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_financials_unique ON stock_financials(ticker, report_date, statement_type, metric_name);
    CREATE INDEX IF NOT EXISTS idx_stock_financials_ticker_date ON stock_financials(ticker, report_date DESC);

    CREATE TABLE IF NOT EXISTS uploaded_files (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      file_name TEXT NOT NULL,
      file_type TEXT NOT NULL,
      upload_source TEXT,
      file_status TEXT NOT NULL,
      uploaded_at TEXT NOT NULL,
      imported_at TEXT,
      notes TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_uploaded_files_ticker_time ON uploaded_files(ticker, uploaded_at DESC);

    CREATE TABLE IF NOT EXISTS model_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      run_time TEXT NOT NULL,
      model_version TEXT,
      train_start TEXT,
      train_end TEXT,
      sample_count INTEGER DEFAULT 0,
      feature_count INTEGER DEFAULT 0,
      status TEXT NOT NULL,
      notes TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_model_runs_ticker_time ON model_runs(ticker, run_time DESC);

    CREATE TABLE IF NOT EXISTS prediction_results (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      prediction_month TEXT NOT NULL,
      predicted_return REAL,
      up_probability REAL,
      down_probability REAL,
      signal TEXT,
      actual_return REAL,
      strategy_return REAL,
      cumulative_strategy REAL,
      cumulative_buy_hold REAL,
      run_id INTEGER,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_prediction_results_ticker_month ON prediction_results(ticker, prediction_month DESC);
    CREATE INDEX IF NOT EXISTS idx_prediction_results_run_id ON prediction_results(run_id);

    CREATE TABLE IF NOT EXISTS feature_importance (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      run_id INTEGER,
      prediction_month TEXT,
      feature_name TEXT NOT NULL,
      importance REAL,
      rank_num INTEGER,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_feature_importance_ticker_run ON feature_importance(ticker, run_id, rank_num);

    CREATE TABLE IF NOT EXISTS latest_signals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL UNIQUE,
      latest_month TEXT,
      predicted_return REAL,
      up_probability REAL,
      down_probability REAL,
      signal TEXT,
      direction_accuracy REAL,
      precision_score REAL,
      recall_score REAL,
      f1_score REAL,
      mae REAL,
      rmse REAL,
      strategy_cagr REAL,
      buy_hold_cagr REAL,
      max_drawdown REAL,
      sharpe_ratio REAL,
      run_id INTEGER,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS markets (
      id TEXT PRIMARY KEY,
      event_id TEXT,
      title TEXT NOT NULL,
      category TEXT,
      resolution_source TEXT,
      end_time TEXT,
      enable_order_book INTEGER DEFAULT 1,
      status TEXT DEFAULT 'active',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_markets_status ON markets(status, updated_at DESC);

    CREATE TABLE IF NOT EXISTS outcomes (
      id TEXT PRIMARY KEY,
      market_id TEXT NOT NULL,
      label TEXT NOT NULL,
      token_id TEXT NOT NULL UNIQUE,
      side TEXT,
      settlement_rule TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(market_id) REFERENCES markets(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_outcomes_market ON outcomes(market_id);

    CREATE TABLE IF NOT EXISTS orderbook_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      asset_id TEXT NOT NULL,
      best_bid REAL,
      best_ask REAL,
      bid_levels_json TEXT,
      ask_levels_json TEXT,
      depth_1 REAL,
      depth_5 REAL
    );
    CREATE INDEX IF NOT EXISTS idx_orderbook_asset_ts ON orderbook_snapshots(asset_id, ts DESC);

    CREATE TABLE IF NOT EXISTS trade_ticks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      asset_id TEXT NOT NULL,
      price REAL,
      size REAL,
      side TEXT,
      aggressor TEXT,
      tx_hash TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_trade_ticks_asset_ts ON trade_ticks(asset_id, ts DESC);

    CREATE TABLE IF NOT EXISTS relation_graph (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      left_outcome_id TEXT NOT NULL,
      right_outcome_id TEXT NOT NULL,
      relation_type TEXT NOT NULL,
      confidence REAL,
      source_rule TEXT,
      verified INTEGER DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_relation_graph_outcomes ON relation_graph(left_outcome_id, right_outcome_id);

    CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
      id TEXT PRIMARY KEY,
      strategy_type TEXT NOT NULL,
      discovered_at TEXT NOT NULL,
      theory_profit REAL,
      vwap_profit REAL,
      confidence REAL,
      legs_json TEXT NOT NULL,
      status TEXT NOT NULL,
      market_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_arb_opp_status_time ON arbitrage_opportunities(status, discovered_at DESC);

    CREATE TABLE IF NOT EXISTS trading_accounts (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      account_type TEXT,
      display_name TEXT,
      signer_address TEXT,
      funder_address TEXT,
      signature_type TEXT,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_trading_accounts_user ON trading_accounts(user_id, updated_at DESC);

    CREATE TABLE IF NOT EXISTS polymarket_api_credentials (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL UNIQUE,
      api_key_enc TEXT NOT NULL,
      secret_enc TEXT NOT NULL,
      passphrase_enc TEXT NOT NULL,
      key_version TEXT,
      status TEXT NOT NULL,
      last_verified_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(account_id) REFERENCES trading_accounts(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS account_balances (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      asset_symbol TEXT NOT NULL,
      available_balance REAL DEFAULT 0,
      locked_balance REAL DEFAULT 0,
      snapshot_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_account_balances_account_ts ON account_balances(account_id, snapshot_at DESC);

    CREATE TABLE IF NOT EXISTS account_positions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      market_id TEXT,
      token_id TEXT,
      qty REAL DEFAULT 0,
      avg_cost REAL,
      mark_price REAL,
      unrealized_pnl REAL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_account_positions_account ON account_positions(account_id, updated_at DESC);

    CREATE TABLE IF NOT EXISTS risk_limits (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL UNIQUE,
      max_daily_notional REAL DEFAULT 0,
      max_order_notional REAL DEFAULT 50,
      max_market_exposure REAL DEFAULT 0,
      max_slippage_bps REAL DEFAULT 100,
      max_open_orders INTEGER DEFAULT 10,
      auto_trading INTEGER DEFAULT 0,
      min_expected_edge_bps REAL DEFAULT 40,
      min_model_confidence REAL DEFAULT 0.65,
      min_orderbook_depth REAL DEFAULT 300,
      order_cooldown_sec INTEGER DEFAULT 30,
      max_daily_realized_loss REAL DEFAULT 80,
      max_consecutive_failed_orders INTEGER DEFAULT 5,
      max_reject_ratio_pct_10m REAL DEFAULT 40,
      halt_on_api_degraded INTEGER DEFAULT 1,
      default_order_type TEXT DEFAULT 'GTC',
      post_only INTEGER DEFAULT 1,
      allow_taker INTEGER DEFAULT 0,
      cancel_stale_after_sec INTEGER DEFAULT 120,
      paper_mode INTEGER DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_risk_limits_account ON risk_limits(account_id, updated_at DESC);

    CREATE TABLE IF NOT EXISTS risk_limits_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      snapshot_json TEXT NOT NULL,
      actor TEXT,
      reason TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_risk_limits_history_account ON risk_limits_history(account_id, created_at DESC);

    CREATE TABLE IF NOT EXISTS executions (
      id TEXT PRIMARY KEY,
      arb_id TEXT NOT NULL,
      account_id TEXT NOT NULL,
      status TEXT NOT NULL,
      expected_profit REAL,
      realized_profit REAL,
      latency_ms INTEGER,
      abort_reason TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_executions_account_time ON executions(account_id, created_at DESC);

    CREATE TABLE IF NOT EXISTS execution_legs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      execution_id TEXT NOT NULL,
      leg_index INTEGER,
      market_id TEXT,
      token_id TEXT,
      side TEXT,
      qty REAL,
      limit_price REAL,
      fill_qty REAL,
      avg_fill_price REAL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(execution_id) REFERENCES executions(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_execution_legs_execution ON execution_legs(execution_id, leg_index);

    CREATE TABLE IF NOT EXISTS authorization_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      action_type TEXT NOT NULL,
      result TEXT NOT NULL,
      metadata_json TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_authorization_logs_account ON authorization_logs(account_id, created_at DESC);

    CREATE TABLE IF NOT EXISTS audit_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      actor TEXT,
      object_type TEXT NOT NULL,
      object_id TEXT,
      action TEXT NOT NULL,
      result TEXT NOT NULL,
      metadata_json TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_audit_logs_time ON audit_logs(created_at DESC);

    CREATE TABLE IF NOT EXISTS strategy_configs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      strategy_id TEXT NOT NULL UNIQUE,
      enabled INTEGER DEFAULT 0,
      params_json TEXT,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS ledger_entries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      execution_id TEXT,
      execution_leg_index INTEGER,
      activity_type TEXT NOT NULL,
      market_id TEXT,
      token_id TEXT,
      strategy_type TEXT,
      usdc_delta REAL DEFAULT 0,
      token_delta REAL DEFAULT 0,
      fee_delta REAL DEFAULT 0,
      rebate_delta REAL DEFAULT 0,
      reward_delta REAL DEFAULT 0,
      tx_hash TEXT,
      fill_id TEXT,
      occurred_at TEXT NOT NULL,
      source_tag TEXT DEFAULT 'execution_derived',
      metadata_json TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_time ON ledger_entries(account_id, occurred_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ledger_entries_market ON ledger_entries(account_id, market_id, occurred_at DESC);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_entries_unique_leg
      ON ledger_entries(account_id, execution_id, execution_leg_index, activity_type);

    CREATE TABLE IF NOT EXISTS position_marks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      market_id TEXT,
      token_id TEXT,
      mark_price REAL,
      mark_source TEXT DEFAULT 'account_positions',
      qty REAL DEFAULT 0,
      unrealized_pnl REAL DEFAULT 0,
      ts TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_position_marks_account_ts ON position_marks(account_id, ts DESC);

    CREATE TABLE IF NOT EXISTS pnl_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      ts TEXT NOT NULL,
      realized_pnl REAL DEFAULT 0,
      unrealized_pnl REAL DEFAULT 0,
      fee_total REAL DEFAULT 0,
      rebate_total REAL DEFAULT 0,
      reward_total REAL DEFAULT 0,
      total_pnl REAL DEFAULT 0,
      calc_version TEXT DEFAULT 'v1',
      metadata_json TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_pnl_snapshots_account_ts ON pnl_snapshots(account_id, ts DESC);

    CREATE TABLE IF NOT EXISTS reconciliation_results (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      ts TEXT NOT NULL,
      internal_total_pnl REAL DEFAULT 0,
      positions_total_pnl REAL DEFAULT 0,
      leaderboard_total_pnl REAL,
      diff_internal_vs_positions REAL DEFAULT 0,
      diff_internal_vs_leaderboard REAL,
      status TEXT DEFAULT 'ok',
      notes TEXT,
      metadata_json TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_reconciliation_results_account_ts ON reconciliation_results(account_id, ts DESC);

    CREATE TABLE IF NOT EXISTS strategy_attribution (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      strategy_id TEXT NOT NULL,
      strategy_type TEXT,
      volume REAL DEFAULT 0,
      realized_pnl REAL DEFAULT 0,
      unrealized_pnl REAL DEFAULT 0,
      n_trades INTEGER DEFAULT 0,
      ts TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_strategy_attr_account_ts ON strategy_attribution(account_id, ts DESC);

    CREATE TABLE IF NOT EXISTS polymarket_top_trader_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_tag TEXT NOT NULL,
      rank_num INTEGER,
      wallet TEXT NOT NULL,
      username TEXT,
      pnl REAL DEFAULT 0,
      vol REAL DEFAULT 0,
      order_by TEXT,
      time_period TEXT,
      fetched_at TEXT NOT NULL,
      summary_json TEXT,
      data_sources_json TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_poly_top_trader_run_rank ON polymarket_top_trader_snapshots(run_tag, rank_num);
    CREATE INDEX IF NOT EXISTS idx_poly_top_trader_wallet_time ON polymarket_top_trader_snapshots(wallet, fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_poly_top_trader_fetch_time ON polymarket_top_trader_snapshots(fetched_at DESC);

    CREATE TABLE IF NOT EXISTS regime_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      as_of_date TEXT NOT NULL,
      regime_code TEXT NOT NULL,
      regime_label TEXT NOT NULL,
      confidence REAL DEFAULT 0,
      score REAL,
      score_delta REAL,
      drivers_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(as_of_date, regime_code)
    );
    CREATE INDEX IF NOT EXISTS idx_regime_snapshots_asof ON regime_snapshots(as_of_date DESC);

    CREATE TABLE IF NOT EXISTS alert_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      as_of_date TEXT NOT NULL,
      dimension_code TEXT,
      alert_code TEXT NOT NULL,
      alert_level TEXT NOT NULL,
      alert_title TEXT NOT NULL,
      trigger_value TEXT,
      threshold_rule TEXT,
      commentary TEXT,
      payload_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_alert_snapshots_asof ON alert_snapshots(as_of_date DESC);
    CREATE INDEX IF NOT EXISTS idx_alert_snapshots_level ON alert_snapshots(alert_level);

    CREATE TABLE IF NOT EXISTS geopolitical_overlays (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      as_of_date TEXT NOT NULL UNIQUE,
      conflict_level TEXT,
      supply_disruption_level TEXT,
      shipping_risk_level TEXT,
      oil_shock_scenario TEXT,
      inflation_impact TEXT,
      risk_asset_impact TEXT,
      credit_impact TEXT,
      summary TEXT,
      payload_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS portfolio_watchlists (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_email TEXT NOT NULL,
      list_name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(user_email, list_name)
    );

    CREATE TABLE IF NOT EXISTS portfolio_positions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      watchlist_id INTEGER NOT NULL,
      ticker TEXT NOT NULL,
      quantity REAL DEFAULT 0,
      cost_basis REAL,
      market_value REAL,
      note TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(watchlist_id, ticker),
      FOREIGN KEY(watchlist_id) REFERENCES portfolio_watchlists(id)
    );
    CREATE INDEX IF NOT EXISTS idx_portfolio_positions_watchlist ON portfolio_positions(watchlist_id);

    CREATE TABLE IF NOT EXISTS stock_macro_exposures (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL UNIQUE,
      interest_rate_sensitivity TEXT,
      growth_sensitivity TEXT,
      inflation_sensitivity TEXT,
      oil_sensitivity TEXT,
      credit_sensitivity TEXT,
      usd_sensitivity TEXT,
      geopolitics_sensitivity TEXT,
      volatility_sensitivity TEXT,
      payload_json TEXT,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS stock_macro_signals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      as_of_date TEXT NOT NULL,
      ticker TEXT NOT NULL,
      regime_code TEXT,
      macro_risk_score REAL,
      signal TEXT,
      action_bias TEXT,
      explanation_short TEXT,
      explanation_long TEXT,
      payload_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(as_of_date, ticker)
    );
    CREATE INDEX IF NOT EXISTS idx_stock_macro_signals_asof ON stock_macro_signals(as_of_date DESC);
    CREATE INDEX IF NOT EXISTS idx_stock_macro_signals_ticker ON stock_macro_signals(ticker);

    CREATE TABLE IF NOT EXISTS market_transmission_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      as_of_date TEXT NOT NULL UNIQUE,
      rates_bias TEXT,
      equities_bias TEXT,
      credit_bias TEXT,
      usd_bias TEXT,
      commodities_bias TEXT,
      crypto_bias TEXT,
      sectors_json TEXT,
      asset_classes_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS daily_action_biases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      as_of_date TEXT NOT NULL UNIQUE,
      overall_bias TEXT NOT NULL,
      favored_styles_json TEXT,
      avoided_styles_json TEXT,
      favored_sectors_json TEXT,
      avoided_sectors_json TEXT,
      summary TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
  )
  conn.executescript(
    """
    CREATE TABLE IF NOT EXISTS scoring_calibration_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      rule_code TEXT UNIQUE NOT NULL,
      target_type TEXT NOT NULL,
      target_code TEXT NOT NULL,
      transform_method TEXT NOT NULL,
      static_weight REAL NOT NULL DEFAULT 0.40,
      momentum_weight REAL NOT NULL DEFAULT 0.35,
      resonance_weight REAL NOT NULL DEFAULT 0.25,
      tail_penalty_enabled INTEGER NOT NULL DEFAULT 1,
      green_threshold REAL,
      yellow_threshold REAL,
      orange_threshold REAL,
      red_threshold REAL,
      config_json TEXT NOT NULL DEFAULT '{}',
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS dimension_scores (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id INTEGER NOT NULL,
      dimension_code TEXT NOT NULL,
      layer TEXT,
      static_level_score REAL,
      momentum_score REAL,
      resonance_penalty REAL DEFAULT 0,
      tail_penalty REAL DEFAULT 0,
      pre_overlay_score REAL,
      final_dimension_score REAL,
      signal_direction TEXT,
      alert_color TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(run_id, dimension_code)
    );
    CREATE INDEX IF NOT EXISTS idx_dimension_scores_run_layer ON dimension_scores(run_id, layer);

    CREATE TABLE IF NOT EXISTS dimension_resonance_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id INTEGER NOT NULL,
      dimension_code TEXT NOT NULL,
      resonance_score REAL NOT NULL DEFAULT 0,
      resonance_flag INTEGER NOT NULL DEFAULT 0,
      triggered_rules_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(run_id, dimension_code)
    );

    CREATE TABLE IF NOT EXISTS regime_layer_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id INTEGER NOT NULL,
      layer TEXT NOT NULL,
      layer_score REAL NOT NULL,
      layer_regime TEXT,
      layer_confidence REAL,
      key_drivers_json TEXT NOT NULL DEFAULT '[]',
      key_risks_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(run_id, layer)
    );
    CREATE INDEX IF NOT EXISTS idx_regime_layer_snapshots_run ON regime_layer_snapshots(run_id);

    CREATE TABLE IF NOT EXISTS overlay_decisions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id INTEGER NOT NULL,
      overlay_type TEXT NOT NULL,
      overlay_level TEXT NOT NULL,
      override_applied INTEGER NOT NULL DEFAULT 0,
      score_cap REAL,
      regime_override TEXT,
      alert_override TEXT,
      rationale TEXT,
      triggered_signals_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_overlay_decisions_run ON overlay_decisions(run_id);

    CREATE TABLE IF NOT EXISTS geopolitical_overlay_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id INTEGER NOT NULL UNIQUE,
      conflict_intensity_score REAL,
      supply_disruption_score REAL,
      shipping_insurance_score REAL,
      energy_microstructure_score REAL,
      macro_transmission_score REAL,
      overlay_level TEXT NOT NULL,
      brent_price REAL,
      brent_5d_change_pct REAL,
      hormuz_status TEXT,
      conclusion TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_geopolitical_overlay_run ON geopolitical_overlay_snapshots(run_id);

    CREATE TABLE IF NOT EXISTS daily_analysis (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id INTEGER NOT NULL UNIQUE,
      as_of_date TEXT NOT NULL,
      score_background REAL,
      final_regime TEXT,
      final_alert_level TEXT,
      decision_priority TEXT,
      signal_confidence_score REAL,
      overlay_summary TEXT,
      action_size_cap REAL,
      hedge_preference TEXT,
      payload_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_daily_analysis_asof ON daily_analysis(as_of_date DESC, id DESC);

    CREATE TABLE IF NOT EXISTS portfolio_macro_exposure (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT,
      ticker TEXT NOT NULL,
      dynamic_rate_beta REAL,
      dynamic_growth_beta REAL,
      dynamic_inflation_beta REAL,
      dynamic_oil_beta REAL,
      dynamic_credit_beta REAL,
      dynamic_usd_beta REAL,
      dynamic_vol_beta REAL,
      valuation_stretch_score REAL,
      drawdown_sensitivity REAL,
      earnings_revision_sensitivity REAL,
      exposure_version TEXT DEFAULT 'v2.1',
      payload_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(user_id, ticker)
    );

    CREATE TABLE IF NOT EXISTS portfolio_risk_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id INTEGER NOT NULL,
      user_id TEXT NOT NULL,
      regime_confidence REAL,
      signal_confidence_score REAL,
      hedge_preference TEXT,
      action_size_cap REAL,
      dynamic_top_risk_positions_json TEXT DEFAULT '[]',
      payload_json TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(run_id, user_id)
    );
    CREATE INDEX IF NOT EXISTS idx_portfolio_risk_snapshots_user ON portfolio_risk_snapshots(user_id, created_at DESC);
    """
  )
  for ddl in [
    "ALTER TABLE daily_reports ADD COLUMN ai_short_summary TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_detailed_interpretation TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_short_summary_zh TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_short_summary_en TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_detailed_interpretation_zh TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_detailed_interpretation_en TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_model TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_status TEXT",
    "ALTER TABLE daily_reports ADD COLUMN ai_generated_at TEXT",
    "ALTER TABLE risk_limits ADD COLUMN max_order_notional REAL DEFAULT 50",
    "ALTER TABLE risk_limits ADD COLUMN min_expected_edge_bps REAL DEFAULT 40",
    "ALTER TABLE risk_limits ADD COLUMN min_model_confidence REAL DEFAULT 0.65",
    "ALTER TABLE risk_limits ADD COLUMN min_orderbook_depth REAL DEFAULT 300",
    "ALTER TABLE risk_limits ADD COLUMN order_cooldown_sec INTEGER DEFAULT 30",
    "ALTER TABLE risk_limits ADD COLUMN max_daily_realized_loss REAL DEFAULT 80",
    "ALTER TABLE risk_limits ADD COLUMN max_consecutive_failed_orders INTEGER DEFAULT 5",
    "ALTER TABLE risk_limits ADD COLUMN max_reject_ratio_pct_10m REAL DEFAULT 40",
    "ALTER TABLE risk_limits ADD COLUMN halt_on_api_degraded INTEGER DEFAULT 1",
    "ALTER TABLE risk_limits ADD COLUMN default_order_type TEXT DEFAULT 'GTC'",
    "ALTER TABLE risk_limits ADD COLUMN post_only INTEGER DEFAULT 1",
    "ALTER TABLE risk_limits ADD COLUMN allow_taker INTEGER DEFAULT 0",
    "ALTER TABLE risk_limits ADD COLUMN cancel_stale_after_sec INTEGER DEFAULT 120",
    "ALTER TABLE risk_limits ADD COLUMN paper_mode INTEGER DEFAULT 1",
    "ALTER TABLE trading_accounts ADD COLUMN display_name TEXT",
    "ALTER TABLE model_runs ADD COLUMN run_type TEXT",
    "ALTER TABLE model_runs ADD COLUMN as_of_date TEXT",
    "ALTER TABLE model_runs ADD COLUMN total_score REAL",
    "ALTER TABLE model_runs ADD COLUMN score_background REAL",
    "ALTER TABLE model_runs ADD COLUMN normalized_score REAL",
    "ALTER TABLE model_runs ADD COLUMN final_regime TEXT",
    "ALTER TABLE model_runs ADD COLUMN regime_confidence REAL",
    "ALTER TABLE model_runs ADD COLUMN overlay_level TEXT",
    "ALTER TABLE model_runs ADD COLUMN overlay_override_applied INTEGER DEFAULT 0",
    "ALTER TABLE model_runs ADD COLUMN score_cap_applied REAL",
    "ALTER TABLE model_runs ADD COLUMN primary_decision_source TEXT",
    "ALTER TABLE model_runs ADD COLUMN topline_message TEXT",
    "ALTER TABLE model_runs ADD COLUMN payload_json TEXT",
    "ALTER TABLE stock_macro_signals ADD COLUMN run_id INTEGER",
    "ALTER TABLE stock_macro_signals ADD COLUMN user_id TEXT",
    "ALTER TABLE stock_macro_signals ADD COLUMN regime_contribution REAL",
    "ALTER TABLE stock_macro_signals ADD COLUMN overlay_contribution REAL",
    "ALTER TABLE stock_macro_signals ADD COLUMN valuation_penalty REAL",
    "ALTER TABLE stock_macro_signals ADD COLUMN volatility_penalty REAL",
    "ALTER TABLE stock_macro_signals ADD COLUMN recommendation TEXT",
    "ALTER TABLE stock_macro_signals ADD COLUMN recommendation_confidence REAL",
    "ALTER TABLE stock_macro_signals ADD COLUMN suggested_action_size REAL",
    "ALTER TABLE stock_macro_signals ADD COLUMN rationale_json TEXT",
  ]:
    try:
      conn.execute(ddl)
    except sqlite3.OperationalError:
      pass
  conn.commit()
  # ---- One-time cleanups (idempotent, safe to run on every boot) ----
  try:
    # 1) Collapse duplicate macro_v2_1 model_runs per as_of_date.
    #    Keep the most recent row, delete older duplicates and their child
    #    daily_analysis rows.
    cursor = conn.execute(
      """
      SELECT as_of_date,
             COUNT(*) AS c,
             MAX(id) AS keep_id,
             GROUP_CONCAT(id) AS all_ids
      FROM model_runs
      WHERE run_type='macro_v2_1' AND as_of_date IS NOT NULL AND as_of_date <> ''
      GROUP BY as_of_date HAVING c > 1
      """
    )
    dup_dates = list(cursor.fetchall())
    for r in dup_dates:
      keep = int(r["keep_id"])
      ids = [int(x) for x in str(r["all_ids"]).split(",") if x]
      drop = [i for i in ids if i != keep]
      if drop:
        placeholder = ",".join("?" for _ in drop)
        conn.execute(f"DELETE FROM daily_analysis WHERE run_id IN ({placeholder})", drop)
        conn.execute(f"DELETE FROM model_runs WHERE id IN ({placeholder})", drop)
    if dup_dates:
      conn.commit()

    # 2) TTL prune for high-velocity polymarket arbitrage_opportunities.
    #    Keep latest 7 days; older rows are pure history bloat.
    #    Table grows ~31K rows/day on prod; 60d retained ~1M rows ≈ 1.3GB DB.
    conn.execute(
      """
      DELETE FROM arbitrage_opportunities
      WHERE discovered_at IS NOT NULL
        AND discovered_at < datetime('now', '-7 days')
      """
    )
    # 3) TTL prune for token usage telemetry (kept 90 days).
    conn.execute(
      """
      DELETE FROM monitor_token_usage
      WHERE logged_at IS NOT NULL
        AND logged_at < datetime('now', '-90 days')
      """
    )
    conn.commit()
  except Exception:
    # Cleanup is best-effort; never block server startup.
    pass
  conn.close()


def create_macro_model_run(as_of_date: str, run_payload: dict):
  """Create or update the macro_v2_1 run for a given as_of_date.

  Previously this always INSERTed a new row, which caused two model_runs
  per day (one from fetch-only cron at 08:00, one from report-only cron
  at 09:00) and downstream produced duplicate daily_analysis rows.

  Now we UPSERT by (run_type='macro_v2_1', as_of_date) so each calendar
  day has exactly one macro run_id. The latest invocation's payload wins.
  """
  conn = get_conn()
  try:
    ts = now_iso()
    payload = run_payload if isinstance(run_payload, dict) else {}
    date_key = str(as_of_date or "").strip()
    existing = conn.execute(
      "SELECT id FROM model_runs WHERE run_type='macro_v2_1' AND as_of_date=? ORDER BY id DESC LIMIT 1",
      (date_key,),
    ).fetchone()
    if existing and int(existing["id"] or 0) > 0:
      run_id = int(existing["id"])
      conn.execute(
        """
        UPDATE model_runs SET
          run_time=?,
          model_version='macro-v2.1',
          train_start=?,
          train_end=?,
          status=?,
          notes=?,
          total_score=?,
          score_background=?,
          normalized_score=?,
          final_regime=?,
          regime_confidence=?,
          overlay_level=?,
          overlay_override_applied=?,
          score_cap_applied=?,
          primary_decision_source=?,
          topline_message=?,
          payload_json=?
        WHERE id=?
        """,
        (
          ts,
          ts,
          ts,
          str(payload.get("status") or "ok"),
          str(payload.get("notes") or ""),
          float(payload.get("total_score") or 0),
          float(payload.get("score_background") or 0),
          float(payload.get("normalized_score") or 0),
          str(payload.get("final_regime") or ""),
          float(payload.get("regime_confidence") or 0),
          str(payload.get("overlay_level") or ""),
          1 if payload.get("overlay_override_applied") else 0,
          payload.get("score_cap_applied"),
          str(payload.get("primary_decision_source") or ""),
          str(payload.get("topline_message") or ""),
          json.dumps(payload.get("payload") or {}, ensure_ascii=False),
          run_id,
        ),
      )
      conn.commit()
      return run_id
    cur = conn.execute(
      """
      INSERT INTO model_runs
      (ticker, run_time, model_version, train_start, train_end, sample_count, feature_count, status, notes,
       run_type, as_of_date, total_score, score_background, normalized_score, final_regime, regime_confidence,
       overlay_level, overlay_override_applied, score_cap_applied, primary_decision_source, topline_message, payload_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      (
        "__MACRO__",
        ts,
        "macro-v2.1",
        ts,
        ts,
        0,
        0,
        str(payload.get("status") or "ok"),
        str(payload.get("notes") or ""),
        "macro_v2_1",
        date_key,
        float(payload.get("total_score") or 0),
        float(payload.get("score_background") or 0),
        float(payload.get("normalized_score") or 0),
        str(payload.get("final_regime") or ""),
        float(payload.get("regime_confidence") or 0),
        str(payload.get("overlay_level") or ""),
        1 if payload.get("overlay_override_applied") else 0,
        payload.get("score_cap_applied"),
        str(payload.get("primary_decision_source") or ""),
        str(payload.get("topline_message") or ""),
        json.dumps(payload.get("payload") or {}, ensure_ascii=False),
      ),
    )
    run_id = int(cur.lastrowid or 0)
    conn.commit()
    return run_id
  finally:
    conn.close()


def get_macro_model_run(as_of_date: str):
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT id, run_time, as_of_date, total_score, score_background, normalized_score, final_regime,
             regime_confidence, overlay_level, overlay_override_applied, score_cap_applied,
             primary_decision_source, topline_message, payload_json
      FROM model_runs
      WHERE run_type='macro_v2_1' AND as_of_date=?
      ORDER BY id DESC
      LIMIT 1
      """,
      (str(as_of_date or "").strip(),),
    ).fetchone()
    if not row:
      return None
    out = dict(row)
    try:
      out["payload_json"] = json.loads(out.get("payload_json") or "{}")
    except Exception:
      out["payload_json"] = {}
    return out
  finally:
    conn.close()


def get_latest_macro_model_run():
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT id, run_time, as_of_date, total_score, score_background, normalized_score, final_regime,
             regime_confidence, overlay_level, overlay_override_applied, score_cap_applied,
             primary_decision_source, topline_message, payload_json
      FROM model_runs
      WHERE run_type='macro_v2_1'
      ORDER BY id DESC
      LIMIT 1
      """
    ).fetchone()
    if not row:
      return None
    out = dict(row)
    try:
      out["payload_json"] = json.loads(out.get("payload_json") or "{}")
    except Exception:
      out["payload_json"] = {}
    return out
  finally:
    conn.close()


def replace_dimension_scores(run_id: int, rows):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute("DELETE FROM dimension_scores WHERE run_id=?", (int(run_id),))
    for r in rows or []:
      conn.execute(
        """
        INSERT INTO dimension_scores
        (run_id, dimension_code, layer, static_level_score, momentum_score, resonance_penalty, tail_penalty,
         pre_overlay_score, final_dimension_score, signal_direction, alert_color, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
          int(run_id),
          str(r.get("dimension_code") or ""),
          str(r.get("layer") or ""),
          r.get("static_level_score"),
          r.get("momentum_score"),
          r.get("resonance_penalty"),
          r.get("tail_penalty"),
          r.get("pre_overlay_score"),
          r.get("final_dimension_score"),
          str(r.get("signal_direction") or ""),
          str(r.get("alert_color") or ""),
          ts,
          ts,
        ),
      )
    conn.commit()
  finally:
    conn.close()


def replace_regime_layers(run_id: int, rows):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute("DELETE FROM regime_layer_snapshots WHERE run_id=?", (int(run_id),))
    for r in rows or []:
      conn.execute(
        """
        INSERT INTO regime_layer_snapshots
        (run_id, layer, layer_score, layer_regime, layer_confidence, key_drivers_json, key_risks_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
          int(run_id),
          str(r.get("layer") or ""),
          float(r.get("layer_score") or 0),
          str(r.get("layer_regime") or ""),
          float(r.get("layer_confidence") or 0),
          json.dumps(r.get("key_drivers") or [], ensure_ascii=False),
          json.dumps(r.get("key_risks") or [], ensure_ascii=False),
          ts,
        ),
      )
    conn.commit()
  finally:
    conn.close()


def upsert_overlay_decision(run_id: int, row: dict):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute("DELETE FROM overlay_decisions WHERE run_id=?", (int(run_id),))
    conn.execute(
      """
      INSERT INTO overlay_decisions
      (run_id, overlay_type, overlay_level, override_applied, score_cap, regime_override, alert_override, rationale, triggered_signals_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      (
        int(run_id),
        str(row.get("overlay_type") or ""),
        str(row.get("overlay_level") or ""),
        1 if row.get("override_applied") else 0,
        row.get("score_cap"),
        str(row.get("regime_override") or ""),
        str(row.get("alert_override") or ""),
        str(row.get("rationale") or ""),
        json.dumps(row.get("triggered_signals") or [], ensure_ascii=False),
        ts,
      ),
    )
    conn.commit()
  finally:
    conn.close()


def upsert_geopolitical_overlay_snapshot(run_id: int, row: dict):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute("DELETE FROM geopolitical_overlay_snapshots WHERE run_id=?", (int(run_id),))
    conn.execute(
      """
      INSERT INTO geopolitical_overlay_snapshots
      (run_id, conflict_intensity_score, supply_disruption_score, shipping_insurance_score, energy_microstructure_score,
       macro_transmission_score, overlay_level, brent_price, brent_5d_change_pct, hormuz_status, conclusion, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      (
        int(run_id),
        row.get("conflict_intensity_score"),
        row.get("supply_disruption_score"),
        row.get("shipping_insurance_score"),
        row.get("energy_microstructure_score"),
        row.get("macro_transmission_score"),
        str(row.get("overlay_level") or ""),
        row.get("brent_price"),
        row.get("brent_5d_change_pct"),
        str(row.get("hormuz_status") or ""),
        str(row.get("conclusion") or ""),
        ts,
      ),
    )
    conn.commit()
  finally:
    conn.close()


def upsert_daily_analysis_v21(run_id: int, as_of_date: str, row: dict):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO daily_analysis
      (run_id, as_of_date, score_background, final_regime, final_alert_level, decision_priority, signal_confidence_score,
       overlay_summary, action_size_cap, hedge_preference, payload_json, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(run_id) DO UPDATE SET
        score_background=excluded.score_background,
        final_regime=excluded.final_regime,
        final_alert_level=excluded.final_alert_level,
        decision_priority=excluded.decision_priority,
        signal_confidence_score=excluded.signal_confidence_score,
        overlay_summary=excluded.overlay_summary,
        action_size_cap=excluded.action_size_cap,
        hedge_preference=excluded.hedge_preference,
        payload_json=excluded.payload_json,
        updated_at=excluded.updated_at
      """,
      (
        int(run_id),
        str(as_of_date or "").strip(),
        row.get("score_background"),
        str(row.get("final_regime") or ""),
        str(row.get("final_alert_level") or ""),
        str(row.get("decision_priority") or ""),
        row.get("signal_confidence_score"),
        str(row.get("overlay_summary") or ""),
        row.get("action_size_cap"),
        str(row.get("hedge_preference") or ""),
        json.dumps(row.get("payload") or {}, ensure_ascii=False),
        ts,
        ts,
      ),
    )
    conn.commit()
  finally:
    conn.close()


def get_run_id_by_date(as_of_date: str):
  conn = get_conn()
  try:
    row = conn.execute(
      "SELECT id FROM model_runs WHERE run_type='macro_v2_1' AND as_of_date=? ORDER BY id DESC LIMIT 1",
      (str(as_of_date or "").strip(),),
    ).fetchone()
    return int(row["id"]) if row else None
  finally:
    conn.close()


def _get_rows(conn, sql, params=()):
  return [dict(x) for x in conn.execute(sql, params).fetchall()]


def get_regime_layers_by_run_id(run_id: int):
  conn = get_conn()
  try:
    rows = _get_rows(
      conn,
      """
      SELECT layer, layer_score, layer_regime, layer_confidence, key_drivers_json, key_risks_json
      FROM regime_layer_snapshots
      WHERE run_id=?
      ORDER BY CASE layer WHEN 'shock' THEN 1 WHEN 'tactical' THEN 2 WHEN 'cyclical' THEN 3 ELSE 9 END, id ASC
      """,
      (int(run_id),),
    )
    out = []
    for r in rows:
      try:
        r["key_drivers"] = json.loads(r.get("key_drivers_json") or "[]")
      except Exception:
        r["key_drivers"] = []
      try:
        r["key_risks"] = json.loads(r.get("key_risks_json") or "[]")
      except Exception:
        r["key_risks"] = []
      r.pop("key_drivers_json", None)
      r.pop("key_risks_json", None)
      out.append(r)
    return out
  finally:
    conn.close()


def get_overlay_decision_by_run_id(run_id: int):
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT overlay_type, overlay_level, override_applied, score_cap, regime_override, alert_override, rationale, triggered_signals_json
      FROM overlay_decisions
      WHERE run_id=?
      ORDER BY id DESC
      LIMIT 1
      """,
      (int(run_id),),
    ).fetchone()
    if not row:
      return None
    out = dict(row)
    out["override_applied"] = bool(out.get("override_applied"))
    try:
      out["triggered_signals"] = json.loads(out.get("triggered_signals_json") or "[]")
    except Exception:
      out["triggered_signals"] = []
    out.pop("triggered_signals_json", None)
    return out
  finally:
    conn.close()


def get_geopolitical_overlay_snapshot_by_run_id(run_id: int):
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT conflict_intensity_score, supply_disruption_score, shipping_insurance_score, energy_microstructure_score,
             macro_transmission_score, overlay_level, brent_price, brent_5d_change_pct, hormuz_status, conclusion
      FROM geopolitical_overlay_snapshots
      WHERE run_id=?
      LIMIT 1
      """,
      (int(run_id),),
    ).fetchone()
    return dict(row) if row else None
  finally:
    conn.close()


def get_score_calibration_by_run_id(run_id: int):
  conn = get_conn()
  try:
    rows = _get_rows(
      conn,
      """
      SELECT dimension_code, layer, static_level_score, momentum_score, resonance_penalty, tail_penalty,
             pre_overlay_score, final_dimension_score, alert_color
      FROM dimension_scores
      WHERE run_id=?
      ORDER BY dimension_code ASC
      """,
      (int(run_id),),
    )
    return rows
  finally:
    conn.close()


def get_daily_analysis_by_run_id(run_id: int):
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT as_of_date, score_background, final_regime, final_alert_level, decision_priority,
             signal_confidence_score, overlay_summary, action_size_cap, hedge_preference, payload_json
      FROM daily_analysis
      WHERE run_id=?
      LIMIT 1
      """,
      (int(run_id),),
    ).fetchone()
    if not row:
      return None
    out = dict(row)
    try:
      out["payload"] = json.loads(out.get("payload_json") or "{}")
    except Exception:
      out["payload"] = {}
    out.pop("payload_json", None)
    return out
  finally:
    conn.close()


def bind_stock_macro_signals_to_run(as_of_date: str, run_id: int):
  conn = get_conn()
  try:
    conn.execute(
      """
      UPDATE stock_macro_signals
      SET run_id=?
      WHERE as_of_date=?
      """,
      (int(run_id), str(as_of_date or "").strip()),
    )
    conn.commit()
  finally:
    conn.close()


def upsert_portfolio_risk_snapshot(run_id: int, user_id: str, row: dict):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO portfolio_risk_snapshots
      (run_id, user_id, regime_confidence, signal_confidence_score, hedge_preference, action_size_cap,
       dynamic_top_risk_positions_json, payload_json, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(run_id, user_id) DO UPDATE SET
        regime_confidence=excluded.regime_confidence,
        signal_confidence_score=excluded.signal_confidence_score,
        hedge_preference=excluded.hedge_preference,
        action_size_cap=excluded.action_size_cap,
        dynamic_top_risk_positions_json=excluded.dynamic_top_risk_positions_json,
        payload_json=excluded.payload_json,
        updated_at=excluded.updated_at
      """,
      (
        int(run_id),
        str(user_id or "").strip().lower(),
        row.get("regime_confidence"),
        row.get("signal_confidence_score"),
        str(row.get("hedge_preference") or ""),
        row.get("action_size_cap"),
        json.dumps(row.get("dynamic_top_risk_positions") or [], ensure_ascii=False),
        json.dumps(row.get("payload") or {}, ensure_ascii=False),
        ts,
        ts,
      ),
    )
    conn.commit()
  finally:
    conn.close()


def get_portfolio_risk_snapshot(run_id: int, user_id: str):
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT run_id, user_id, regime_confidence, signal_confidence_score, hedge_preference, action_size_cap,
             dynamic_top_risk_positions_json, payload_json, created_at, updated_at
      FROM portfolio_risk_snapshots
      WHERE run_id=? AND user_id=?
      LIMIT 1
      """,
      (int(run_id), str(user_id or "").strip().lower()),
    ).fetchone()
    if not row:
      return None
    out = dict(row)
    try:
      out["dynamic_top_risk_positions"] = json.loads(out.get("dynamic_top_risk_positions_json") or "[]")
    except Exception:
      out["dynamic_top_risk_positions"] = []
    try:
      out["payload"] = json.loads(out.get("payload_json") or "{}")
    except Exception:
      out["payload"] = {}
    out.pop("dynamic_top_risk_positions_json", None)
    out.pop("payload_json", None)
    return out
  finally:
    conn.close()


def get_latest_portfolio_risk_snapshot(user_id: str):
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT prs.run_id, prs.user_id, prs.regime_confidence, prs.signal_confidence_score, prs.hedge_preference, prs.action_size_cap,
             prs.dynamic_top_risk_positions_json, prs.payload_json, prs.created_at, prs.updated_at,
             mr.as_of_date
      FROM portfolio_risk_snapshots prs
      JOIN model_runs mr ON mr.id = prs.run_id
      WHERE prs.user_id=?
      ORDER BY prs.updated_at DESC, prs.id DESC
      LIMIT 1
      """,
      (str(user_id or "").strip().lower(),),
    ).fetchone()
    if not row:
      return None
    out = dict(row)
    try:
      out["dynamic_top_risk_positions"] = json.loads(out.get("dynamic_top_risk_positions_json") or "[]")
    except Exception:
      out["dynamic_top_risk_positions"] = []
    try:
      out["payload"] = json.loads(out.get("payload_json") or "{}")
    except Exception:
      out["payload"] = {}
    out.pop("dynamic_top_risk_positions_json", None)
    out.pop("payload_json", None)
    return out
  finally:
    conn.close()


def get_stock_macro_signal_for_run(run_id: int, ticker: str, user_id: str = ""):
  conn = get_conn()
  try:
    params = [int(run_id), str(ticker or "").strip().upper()]
    sql = """
      SELECT as_of_date, ticker, regime_code, macro_risk_score, signal, action_bias, explanation_short, explanation_long,
             run_id, user_id, regime_contribution, overlay_contribution, valuation_penalty, volatility_penalty,
             recommendation, recommendation_confidence, suggested_action_size, rationale_json, payload_json, created_at
      FROM stock_macro_signals
      WHERE run_id=? AND ticker=?
    """
    uid = str(user_id or "").strip().lower()
    if uid:
      sql += " AND (user_id=? OR user_id IS NULL OR user_id='') ORDER BY CASE WHEN user_id=? THEN 0 ELSE 1 END, id DESC LIMIT 1"
      params.extend([uid, uid])
    else:
      sql += " ORDER BY id DESC LIMIT 1"
    row = conn.execute(sql, tuple(params)).fetchone()
    if not row:
      return None
    out = dict(row)
    for key in ("rationale_json", "payload_json"):
      try:
        out[key] = json.loads(out.get(key) or "{}")
      except Exception:
        out[key] = {}
    return out
  finally:
    conn.close()


def upsert_openrouter_rankings_snapshot(payload: dict):
  conn = get_conn()
  ts = now_iso()
  view = str((payload or {}).get("view") or "week").strip().lower() or "week"
  category = str((payload or {}).get("category") or "all").strip().lower() or "all"
  source_url = str((payload or {}).get("sourceUrl") or "")
  parse_mode = str((payload or {}).get("parseMode") or "")
  fetched_at = str((payload or {}).get("fetchedAt") or ts)

  cur = conn.execute(
    """
    INSERT INTO openrouter_fetch_runs (view, category, source_url, parse_mode, fetched_at, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    (view, category, source_url, parse_mode, fetched_at, ts),
  )
  run_id = int(cur.lastrowid or 0)

  def _insert_rows(table_name: str, rows):
    for r in rows or []:
      conn.execute(
        f"""
        INSERT INTO {table_name}
        (fetch_run_id, rank_num, name, creator, tokens, share, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
          run_id,
          int(r.get("rank") or 0),
          str(r.get("name") or ""),
          str(r.get("creator") or ""),
          str(r.get("tokens") or ""),
          str(r.get("share") or ""),
          ts,
        ),
      )

  _insert_rows("openrouter_top_models", (payload or {}).get("models") or [])
  _insert_rows("openrouter_top_apps", (payload or {}).get("apps") or [])
  _insert_rows("openrouter_top_providers", (payload or {}).get("providers") or [])
  _insert_rows("openrouter_top_prompts", (payload or {}).get("prompts") or [])

  conn.commit()
  conn.close()
  return run_id


def get_latest_openrouter_rankings_snapshot(view: str = "week", category: str = "all"):
  conn = get_conn()
  v = str(view or "week").strip().lower() or "week"
  c = str(category or "all").strip().lower() or "all"
  run = conn.execute(
    """
    SELECT id, view, category, source_url, parse_mode, fetched_at
    FROM openrouter_fetch_runs
    WHERE view = ? AND category = ?
    ORDER BY datetime(replace(replace(fetched_at, 'T', ' '), 'Z', '')) DESC, id DESC
    LIMIT 1
    """,
    (v, c),
  ).fetchone()
  if not run:
    conn.close()
    return None
  run_id = int(run["id"])

  def _rows(table_name: str):
    rows = conn.execute(
      f"""
      SELECT rank_num, name, creator, tokens, share
      FROM {table_name}
      WHERE fetch_run_id = ?
      ORDER BY rank_num ASC, id ASC
      """,
      (run_id,),
    ).fetchall()
    return [
      {
        "rank": int(r["rank_num"] or 0),
        "name": str(r["name"] or ""),
        "creator": str(r["creator"] or ""),
        "tokens": str(r["tokens"] or ""),
        "share": str(r["share"] or ""),
      }
      for r in rows
    ]

  payload = {
    "ok": True,
    "sourceUrl": str(run["source_url"] or ""),
    "parseMode": str(run["parse_mode"] or ""),
    "view": str(run["view"] or v),
    "category": str(run["category"] or c),
    "fetchedAt": str(run["fetched_at"] or ""),
    "models": _rows("openrouter_top_models"),
    "apps": _rows("openrouter_top_apps"),
    "providers": _rows("openrouter_top_providers"),
    "prompts": _rows("openrouter_top_prompts"),
  }
  conn.close()
  return payload


def replace_sheet_rows(sheet_name: str, rows, as_of: str):
  conn = get_conn()
  ts = now_iso()
  conn.execute("DELETE FROM sheet_rows WHERE sheet_name = ? AND as_of = ?", (sheet_name, as_of))
  for idx, row in enumerate(rows or [], start=1):
    conn.execute(
      "INSERT INTO sheet_rows (sheet_name, row_index, row_json, as_of, created_at) VALUES (?, ?, ?, ?, ?)",
      (sheet_name, idx, json.dumps(row, ensure_ascii=False), as_of, ts),
    )
  conn.commit()
  conn.close()


def save_model_snapshot(payload: dict):
  conn = get_conn()
  ts = now_iso()
  as_of = str(payload.get("asOf") or "")
  report_date = str(payload.get("reportDate") or as_of)
  generated_at = str(payload.get("generatedAt") or ts)
  conn.execute(
    "INSERT INTO model_snapshots (as_of, report_date, generated_at, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
    (as_of, report_date, generated_at, json.dumps(payload, ensure_ascii=False), ts),
  )
  conn.commit()
  conn.close()


def get_latest_model_snapshot():
  conn = get_conn()
  rows = conn.execute("SELECT payload_json FROM model_snapshots ORDER BY id DESC LIMIT 50").fetchall()
  conn.close()
  for row in rows:
    try:
      payload = json.loads(row["payload_json"])
    except Exception:
      continue
    if isinstance(payload, dict) and payload:
      return payload
  return None


def upsert_daily_report(report_date: str, text: str, meta: dict, report_path: str = "", payload=None, ai_analysis=None):
  conn = get_conn()
  ts = now_iso()
  existing = conn.execute(
    """
    SELECT score, status, summary, report_path, payload_json,
           ai_short_summary, ai_detailed_interpretation,
           ai_short_summary_zh, ai_short_summary_en,
           ai_detailed_interpretation_zh, ai_detailed_interpretation_en,
           ai_model, ai_status, ai_generated_at
    FROM daily_reports
    WHERE report_date = ?
    """,
    (str(report_date or "").strip(),),
  ).fetchone()
  score = meta.get("score")
  status = meta.get("status")
  summary = meta.get("summary", "")
  ai = ai_analysis if isinstance(ai_analysis, dict) else {}
  has_new_ai = any(
    str(ai.get(k) or "").strip()
    for k in (
      "short_summary",
      "detailed_interpretation",
      "short_summary_zh",
      "short_summary_en",
      "detailed_interpretation_zh",
      "detailed_interpretation_en",
      "model",
      "status",
      "generated_at",
    )
  )
  if has_new_ai:
    ai_short_zh = str(ai.get("short_summary_zh") or "")
    ai_short_en = str(ai.get("short_summary_en") or "")
    ai_detail_zh = str(ai.get("detailed_interpretation_zh") or "")
    ai_detail_en = str(ai.get("detailed_interpretation_en") or "")
    ai_short = str(ai.get("short_summary") or ai_short_zh or ai_short_en or "")
    ai_detail = str(ai.get("detailed_interpretation") or ai_detail_zh or ai_detail_en or "")
    ai_model = str(ai.get("model") or "")
    ai_status = str(ai.get("status") or "")
    ai_generated_at = str(ai.get("generated_at") or "")
  else:
    ai_short = str((existing["ai_short_summary"] if existing else "") or "")
    ai_detail = str((existing["ai_detailed_interpretation"] if existing else "") or "")
    ai_short_zh = str((existing["ai_short_summary_zh"] if existing else "") or "")
    ai_short_en = str((existing["ai_short_summary_en"] if existing else "") or "")
    ai_detail_zh = str((existing["ai_detailed_interpretation_zh"] if existing else "") or "")
    ai_detail_en = str((existing["ai_detailed_interpretation_en"] if existing else "") or "")
    ai_model = str((existing["ai_model"] if existing else "") or "")
    ai_status = str((existing["ai_status"] if existing else "") or "")
    ai_generated_at = str((existing["ai_generated_at"] if existing else "") or "")
  if score is None and existing:
    score = existing["score"]
  if (status is None or str(status).strip() == "") and existing:
    status = existing["status"]
  if (summary is None or str(summary).strip() == "") and existing:
    summary = existing["summary"] or ""
  if (not report_path) and existing:
    report_path = str(existing["report_path"] or "")
  if payload is not None:
    payload_json = json.dumps(payload, ensure_ascii=False)
  else:
    payload_json = existing["payload_json"] if existing else None
  conn.execute(
    """
    INSERT INTO daily_reports
    (report_date, score, status, summary, ai_short_summary, ai_detailed_interpretation, ai_short_summary_zh, ai_short_summary_en, ai_detailed_interpretation_zh, ai_detailed_interpretation_en, ai_model, ai_status, ai_generated_at, report_text, report_path, payload_json, generated_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(report_date) DO UPDATE SET
      score=excluded.score,
      status=excluded.status,
      summary=excluded.summary,
      ai_short_summary=excluded.ai_short_summary,
      ai_detailed_interpretation=excluded.ai_detailed_interpretation,
      ai_short_summary_zh=excluded.ai_short_summary_zh,
      ai_short_summary_en=excluded.ai_short_summary_en,
      ai_detailed_interpretation_zh=excluded.ai_detailed_interpretation_zh,
      ai_detailed_interpretation_en=excluded.ai_detailed_interpretation_en,
      ai_model=excluded.ai_model,
      ai_status=excluded.ai_status,
      ai_generated_at=excluded.ai_generated_at,
      report_text=excluded.report_text,
      report_path=excluded.report_path,
      payload_json=excluded.payload_json,
      generated_at=excluded.generated_at,
      updated_at=excluded.updated_at
    """,
    (
      report_date,
      score,
      status,
      summary,
      ai_short,
      ai_detail,
      ai_short_zh,
      ai_short_en,
      ai_detail_zh,
      ai_detail_en,
      ai_model,
      ai_status,
      ai_generated_at,
      text,
      report_path,
      payload_json,
      ts,
      ts,
      ts,
    ),
  )
  conn.commit()
  conn.close()


def list_daily_reports(limit: int = 200):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT report_date, score, status, summary,
           ai_short_summary, ai_detailed_interpretation,
           ai_short_summary_zh, ai_short_summary_en,
           ai_detailed_interpretation_zh, ai_detailed_interpretation_en,
           ai_model, ai_status, ai_generated_at,
           report_text, report_path, payload_json, generated_at
    FROM daily_reports
    ORDER BY report_date DESC LIMIT ?
    """,
    (limit,),
  ).fetchall()
  conn.close()
  out = []
  for r in rows:
    payload = json.loads(r["payload_json"]) if r["payload_json"] else None
    out.append(
      {
        "date": r["report_date"],
        "meta": {"score": r["score"], "status": r["status"], "summary": r["summary"]},
        "text": r["report_text"],
        "path": r["report_path"],
        "reportPayload": payload,
        "aiAnalysis": {
          "short_summary": r["ai_short_summary"] or "",
          "detailed_interpretation": r["ai_detailed_interpretation"] or "",
          "short_summary_zh": r["ai_short_summary_zh"] or "",
          "short_summary_en": r["ai_short_summary_en"] or "",
          "detailed_interpretation_zh": r["ai_detailed_interpretation_zh"] or "",
          "detailed_interpretation_en": r["ai_detailed_interpretation_en"] or "",
          "model": r["ai_model"] or "",
          "status": r["ai_status"] or "",
          "generated_at": r["ai_generated_at"] or "",
        },
        "generatedAt": r["generated_at"],
      }
    )
  return out


def get_daily_report(report_date: str):
  conn = get_conn()
  r = conn.execute(
    """
    SELECT report_date, score, status, summary,
           ai_short_summary, ai_detailed_interpretation,
           ai_short_summary_zh, ai_short_summary_en,
           ai_detailed_interpretation_zh, ai_detailed_interpretation_en,
           ai_model, ai_status, ai_generated_at,
           report_text, report_path, payload_json, generated_at
    FROM daily_reports
    WHERE report_date = ?
    """,
    (report_date,),
  ).fetchone()
  conn.close()
  if not r:
    return None
  payload = json.loads(r["payload_json"]) if r["payload_json"] else None
  return {
    "date": r["report_date"],
    "meta": {"score": r["score"], "status": r["status"], "summary": r["summary"]},
    "text": r["report_text"],
    "path": r["report_path"],
    "reportPayload": payload,
    "aiAnalysis": {
      "short_summary": r["ai_short_summary"] or "",
      "detailed_interpretation": r["ai_detailed_interpretation"] or "",
      "short_summary_zh": r["ai_short_summary_zh"] or "",
      "short_summary_en": r["ai_short_summary_en"] or "",
      "detailed_interpretation_zh": r["ai_detailed_interpretation_zh"] or "",
      "detailed_interpretation_en": r["ai_detailed_interpretation_en"] or "",
      "model": r["ai_model"] or "",
      "status": r["ai_status"] or "",
      "generated_at": r["ai_generated_at"] or "",
    },
    "generatedAt": r["generated_at"],
  }


def update_daily_report_analysis(
  report_date: str,
  short_summary: str = "",
  detailed_interpretation: str = "",
  short_summary_zh: str = "",
  short_summary_en: str = "",
  detailed_interpretation_zh: str = "",
  detailed_interpretation_en: str = "",
  model: str = "",
  status: str = "",
  generated_at: str = "",
):
  conn = get_conn()
  ts = now_iso()
  ss_zh = str(short_summary_zh or "")
  ss_en = str(short_summary_en or "")
  di_zh = str(detailed_interpretation_zh or "")
  di_en = str(detailed_interpretation_en or "")
  ss = str(short_summary or ss_zh or ss_en or "")
  di = str(detailed_interpretation or di_zh or di_en or "")
  conn.execute(
    """
    UPDATE daily_reports
    SET ai_short_summary = ?,
        ai_detailed_interpretation = ?,
        ai_short_summary_zh = ?,
        ai_short_summary_en = ?,
        ai_detailed_interpretation_zh = ?,
        ai_detailed_interpretation_en = ?,
        ai_model = ?,
        ai_status = ?,
        ai_generated_at = ?,
        updated_at = ?
    WHERE report_date = ?
    """,
    (
      ss,
      di,
      ss_zh,
      ss_en,
      di_zh,
      di_en,
      str(model or ""),
      str(status or ""),
      str(generated_at or ""),
      ts,
      str(report_date or "").strip(),
    ),
  )
  conn.commit()
  conn.close()


def get_daily_report_analysis(report_date: str):
  conn = get_conn()
  row = conn.execute(
    """
    SELECT report_date, ai_short_summary, ai_detailed_interpretation,
           ai_short_summary_zh, ai_short_summary_en,
           ai_detailed_interpretation_zh, ai_detailed_interpretation_en,
           ai_model, ai_status, ai_generated_at, updated_at
    FROM daily_reports
    WHERE report_date = ?
    LIMIT 1
    """,
    (str(report_date or "").strip(),),
  ).fetchone()
  conn.close()
  if not row:
    return None
  return {
    "report_date": row["report_date"],
    "short_summary": row["ai_short_summary"] or "",
    "detailed_interpretation": row["ai_detailed_interpretation"] or "",
    "short_summary_zh": row["ai_short_summary_zh"] or "",
    "short_summary_en": row["ai_short_summary_en"] or "",
    "detailed_interpretation_zh": row["ai_detailed_interpretation_zh"] or "",
    "detailed_interpretation_en": row["ai_detailed_interpretation_en"] or "",
    "model": row["ai_model"] or "",
    "status": row["ai_status"] or "",
    "generated_at": row["ai_generated_at"] or "",
    "updated_at": row["updated_at"] or "",
  }


def save_online_check(checked_at: str, summary: dict, rows):
  conn = get_conn()
  conn.execute(
    "INSERT INTO online_checks (checked_at, summary_json, rows_json) VALUES (?, ?, ?)",
    (checked_at, json.dumps(summary, ensure_ascii=False), json.dumps(rows, ensure_ascii=False)),
  )
  conn.commit()
  conn.close()


def get_latest_online_check():
  conn = get_conn()
  row = conn.execute(
    """
    SELECT checked_at, summary_json, rows_json
    FROM online_checks
    ORDER BY checked_at DESC, id DESC
    LIMIT 1
    """
  ).fetchone()
  conn.close()
  if not row:
    return None
  try:
    summary = json.loads(row["summary_json"] or "{}")
  except Exception:
    summary = {}
  try:
    rows = json.loads(row["rows_json"] or "[]")
  except Exception:
    rows = []
  return {
    "checkedAt": row["checked_at"] or "",
    "summary": summary,
    "rows": rows,
  }


def add_subscriber(email: str, source: str = "web"):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO subscribers (email, status, source, created_at, updated_at)
    VALUES (?, 'active', ?, ?, ?)
    ON CONFLICT(email) DO UPDATE SET
      status='active',
      source=excluded.source,
      updated_at=excluded.updated_at
    """,
    (email.lower().strip(), source, ts, ts),
  )
  conn.commit()
  conn.close()


def deactivate_subscriber(email: str):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    "UPDATE subscribers SET status='inactive', updated_at=? WHERE email=?",
    (ts, str(email or "").strip().lower()),
  )
  conn.commit()
  conn.close()


def list_active_subscribers():
  conn = get_conn()
  rows = conn.execute("SELECT email, created_at, updated_at FROM subscribers WHERE status='active' ORDER BY created_at ASC").fetchall()
  conn.close()
  return [dict(r) for r in rows]


def list_active_subscribers_with_status(report_date: str = ""):
  conn = get_conn()
  rows = conn.execute(
    "SELECT email, created_at, updated_at FROM subscribers WHERE status='active' ORDER BY created_at ASC"
  ).fetchall()
  out = []
  for r in rows:
    email = str(r["email"] or "").strip().lower()
    welcome_row = conn.execute(
      "SELECT created_at FROM email_event_logs WHERE email = ? AND event_type = 'welcome_sent' LIMIT 1",
      (email,),
    ).fetchone()
    latest_daily = conn.execute(
      """
      SELECT report_date, email_type, status, detail, updated_at
      FROM email_delivery_logs
      WHERE email = ? AND email_type = 'daily_report'
      ORDER BY report_date DESC, id DESC
      LIMIT 1
      """,
      (email,),
    ).fetchone()
    today_row = None
    if report_date:
      today_row = conn.execute(
        """
        SELECT report_date, email_type, status, detail, updated_at
        FROM email_delivery_logs
        WHERE email = ? AND report_date = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (email, report_date),
      ).fetchone()
    out.append(
      {
        "email": email,
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
        "welcome_email_sent": bool(welcome_row),
        "welcome_sent_at": welcome_row["created_at"] if welcome_row else "",
        "daily_report_sent_today": bool(today_row and today_row["email_type"] == "daily_report" and today_row["status"] == "sent"),
        "today_email_type": today_row["email_type"] if today_row else "",
        "today_email_status": today_row["status"] if today_row else "",
        "today_email_detail": today_row["detail"] if today_row else "",
        "today_email_updated_at": today_row["updated_at"] if today_row else "",
        "latest_daily_report_date": latest_daily["report_date"] if latest_daily else "",
        "latest_daily_report_status": latest_daily["status"] if latest_daily else "",
        "latest_daily_report_updated_at": latest_daily["updated_at"] if latest_daily else "",
      }
    )
  conn.close()
  return out


def create_invite_code(code: str, max_uses: int = 1, expires_at: str = "", note: str = "", created_by: str = ""):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      """
      INSERT INTO invite_codes (code, status, max_uses, used_count, expires_at, note, created_by, created_at, updated_at)
      VALUES (?, 'active', ?, 0, ?, ?, ?, ?, ?)
      ON CONFLICT(code) DO UPDATE SET
        status='active',
        max_uses=excluded.max_uses,
        expires_at=excluded.expires_at,
        note=excluded.note,
        created_by=excluded.created_by,
        updated_at=excluded.updated_at
      """,
      (
        str(code or "").strip().upper(),
        max(1, int(max_uses or 1)),
        str(expires_at or "").strip(),
        str(note or "").strip(),
        str(created_by or "").strip().lower(),
        ts,
        ts,
      ),
    )
    conn.commit()
    row = conn.execute(
      """
      SELECT code, status, max_uses, used_count, expires_at, note, created_by, created_at, updated_at
      FROM invite_codes
      WHERE code = ?
      """,
      (str(code or "").strip().upper(),),
    ).fetchone()
    return dict(row) if row else None
  finally:
    conn.close()


def list_invite_codes(limit: int = 100):
  conn = get_conn()
  try:
    rows = conn.execute(
      """
      SELECT code, status, max_uses, used_count, expires_at, note, created_by, created_at, updated_at
      FROM invite_codes
      ORDER BY created_at DESC
      LIMIT ?
      """,
      (max(1, int(limit or 100)),),
    ).fetchall()
    return [dict(r) for r in rows]
  finally:
    conn.close()


def get_invite_code(code: str):
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT code, status, max_uses, used_count, expires_at, note, created_by, created_at, updated_at
      FROM invite_codes
      WHERE code = ?
      """,
      (str(code or "").strip().upper(),),
    ).fetchone()
    return dict(row) if row else None
  finally:
    conn.close()


def redeem_invite_code(code: str, email: str):
  conn = get_conn()
  try:
    ts = now_iso()
    c = str(code or "").strip().upper()
    e = str(email or "").strip().lower()
    row = conn.execute(
      """
      SELECT code, status, max_uses, used_count, expires_at
      FROM invite_codes
      WHERE code = ?
      """,
      (c,),
    ).fetchone()
    if not row:
      return {"ok": False, "error": "invite_not_found"}
    d = dict(row)
    if str(d.get("status") or "").lower() != "active":
      return {"ok": False, "error": "invite_inactive"}
    expires_at = str(d.get("expires_at") or "").strip()
    if expires_at and expires_at < ts:
      return {"ok": False, "error": "invite_expired"}
    if int(d.get("used_count") or 0) >= int(d.get("max_uses") or 1):
      return {"ok": False, "error": "invite_exhausted"}

    conn.execute(
      "UPDATE invite_codes SET used_count = used_count + 1, updated_at=? WHERE code=?",
      (ts, c),
    )
    conn.execute(
      "INSERT INTO invite_redemptions (code, email, redeemed_at) VALUES (?, ?, ?)",
      (c, e, ts),
    )
    conn.commit()
    return {"ok": True}
  finally:
    conn.close()


def create_user_account(email: str, password_hash: str, password_salt: str, invite_code: str = "", invited_by: str = ""):
  conn = get_conn()
  try:
    ts = now_iso()
    e = str(email or "").strip().lower()
    conn.execute(
      """
      INSERT INTO user_accounts (email, password_hash, password_salt, status, invited_by, invite_code, created_at, updated_at)
      VALUES (?, ?, ?, 'active', ?, ?, ?, ?)
      ON CONFLICT(email) DO UPDATE SET
        password_hash=excluded.password_hash,
        password_salt=excluded.password_salt,
        status='active',
        invited_by=excluded.invited_by,
        invite_code=excluded.invite_code,
        updated_at=excluded.updated_at
      """,
      (
        e,
        str(password_hash or "").strip(),
        str(password_salt or "").strip(),
        str(invited_by or "").strip().lower(),
        str(invite_code or "").strip().upper(),
        ts,
        ts,
      ),
    )
    conn.commit()
    row = conn.execute(
      """
      SELECT id, email, status, invited_by, invite_code, created_at, updated_at, last_login_at
      FROM user_accounts
      WHERE email = ?
      """,
      (e,),
    ).fetchone()
    return dict(row) if row else None
  finally:
    conn.close()


def get_user_account(email: str):
  conn = get_conn()
  try:
    row = conn.execute(
      """
      SELECT id, email, password_hash, password_salt, status, invited_by, invite_code, created_at, updated_at, last_login_at
      FROM user_accounts
      WHERE email = ?
      """,
      (str(email or "").strip().lower(),),
    ).fetchone()
    return dict(row) if row else None
  finally:
    conn.close()


def touch_user_login(email: str):
  conn = get_conn()
  try:
    ts = now_iso()
    conn.execute(
      "UPDATE user_accounts SET last_login_at=?, updated_at=? WHERE email=?",
      (ts, ts, str(email or "").strip().lower()),
    )
    conn.commit()
  finally:
    conn.close()


def save_email_dispatch_log(payload: dict):
  conn = get_conn()
  conn.execute(
    "INSERT INTO email_dispatch_logs (report_date, generated_at, sent, failed, recipients, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
    (
      payload.get("date"),
      payload.get("generatedAt"),
      int(payload.get("sent", 0)),
      int(payload.get("failed", 0)),
      int(payload.get("recipients", 0)),
      json.dumps(payload, ensure_ascii=False),
    ),
  )
  conn.commit()
  conn.close()


def has_email_event(email: str, event_type: str):
  conn = get_conn()
  row = conn.execute(
    "SELECT 1 AS ok FROM email_event_logs WHERE email = ? AND event_type = ? LIMIT 1",
    (str(email or "").strip().lower(), str(event_type or "").strip().lower()),
  ).fetchone()
  conn.close()
  return bool(row)


def save_email_event(email: str, event_type: str, payload=None):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO email_event_logs (email, event_type, payload_json, created_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(email, event_type) DO NOTHING
    """,
    (
      str(email or "").strip().lower(),
      str(event_type or "").strip().lower(),
      json.dumps(payload or {}, ensure_ascii=False),
      ts,
    ),
  )
  conn.commit()
  conn.close()


def upsert_email_delivery(email: str, report_date: str, email_type: str, status: str, detail: str = ""):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO email_delivery_logs (email, report_date, email_type, status, detail, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(email, report_date, email_type) DO UPDATE SET
      status=excluded.status,
      detail=excluded.detail,
      updated_at=excluded.updated_at
    """,
    (
      str(email or "").strip().lower(),
      str(report_date or "").strip(),
      str(email_type or "").strip().lower(),
      str(status or "").strip().lower(),
      str(detail or "")[:500],
      ts,
      ts,
    ),
  )
  conn.commit()
  conn.close()


def log_page_event(path: str, referrer: str = "", user_agent: str = "", ip: str = "", visited_at: str = ""):
  conn = get_conn()
  ts = visited_at or now_iso()
  conn.execute(
    "INSERT INTO monitor_page_events (path, referrer, user_agent, ip, visited_at) VALUES (?, ?, ?, ?, ?)",
    (path, referrer, user_agent, ip, ts),
  )
  conn.commit()
  conn.close()


def log_token_usage(source: str, model: str = "", input_tokens: int = 0, output_tokens: int = 0, total_tokens: int = 0, meta=None, logged_at: str = ""):
  conn = get_conn()
  ts = logged_at or now_iso()
  payload = json.dumps(meta or {}, ensure_ascii=False)
  conn.execute(
    """
    INSERT INTO monitor_token_usage (source, model, input_tokens, output_tokens, total_tokens, meta_json, logged_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
    (source, model, int(input_tokens or 0), int(output_tokens or 0), int(total_tokens or 0), payload, ts),
  )
  conn.commit()
  conn.close()


def delete_token_usage_by_source(source: str, start_iso: str = "", end_iso: str = ""):
  conn = get_conn()
  if start_iso and end_iso:
    conn.execute(
      "DELETE FROM monitor_token_usage WHERE source = ? AND logged_at >= ? AND logged_at < ?",
      (source, start_iso, end_iso),
    )
  elif start_iso:
    conn.execute(
      "DELETE FROM monitor_token_usage WHERE source = ? AND logged_at >= ?",
      (source, start_iso),
    )
  else:
    conn.execute("DELETE FROM monitor_token_usage WHERE source = ?", (source,))
  conn.commit()
  conn.close()


def upsert_api_key(service: str, api_key: str):
  conn = get_conn()
  ts = now_iso()
  conn.execute(
    """
    INSERT INTO api_credentials (service, api_key, created_at, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(service) DO UPDATE SET
      api_key=excluded.api_key,
      updated_at=excluded.updated_at
    """,
    (str(service or "").strip().lower(), str(api_key or "").strip(), ts, ts),
  )
  conn.commit()
  conn.close()


def get_api_key(service: str):
  conn = get_conn()
  row = conn.execute(
    "SELECT api_key FROM api_credentials WHERE service = ? LIMIT 1",
    (str(service or "").strip().lower(),),
  ).fetchone()
  conn.close()
  if not row:
    return ""
  return str(row["api_key"] or "").strip()


def upsert_daily_report_ai_insight(
  report_date: str,
  short_summary: str = "",
  detailed_text: str = "",
  insight=None,
  status: str = "ok",
  model: str = "",
  prompt_version: str = "",
  generated_at: str = "",
  error: str = "",
):
  conn = get_conn()
  ts = now_iso()
  insight_json = json.dumps(insight or {}, ensure_ascii=False)
  conn.execute(
    """
    INSERT INTO daily_report_ai_insights
    (report_date, short_summary, detailed_text, insight_json, status, model, prompt_version, generated_at, error, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(report_date) DO UPDATE SET
      short_summary=excluded.short_summary,
      detailed_text=excluded.detailed_text,
      insight_json=excluded.insight_json,
      status=excluded.status,
      model=excluded.model,
      prompt_version=excluded.prompt_version,
      generated_at=excluded.generated_at,
      error=excluded.error,
      updated_at=excluded.updated_at
    """,
    (
      str(report_date or "").strip(),
      str(short_summary or ""),
      str(detailed_text or ""),
      insight_json,
      str(status or "").strip() or "ok",
      str(model or "").strip(),
      str(prompt_version or "").strip(),
      str(generated_at or "").strip() or ts,
      str(error or "")[:500],
      ts,
      ts,
    ),
  )
  conn.commit()
  conn.close()


def get_daily_report_ai_insight(report_date: str):
  conn = get_conn()
  row = conn.execute(
    """
    SELECT report_date, short_summary, detailed_text, insight_json, status, model, prompt_version, generated_at, error
    FROM daily_report_ai_insights
    WHERE report_date = ?
    LIMIT 1
    """,
    (str(report_date or "").strip(),),
  ).fetchone()
  conn.close()
  if not row:
    return None
  insight = {}
  try:
    insight = json.loads(row["insight_json"]) if row["insight_json"] else {}
  except Exception:
    insight = {}
  return {
    "report_date": row["report_date"],
    "short_summary": row["short_summary"] or "",
    "detailed_text": row["detailed_text"] or "",
    "insight": insight,
    "status": row["status"] or "",
    "model": row["model"] or "",
    "prompt_version": row["prompt_version"] or "",
    "generated_at": row["generated_at"] or "",
    "error": row["error"] or "",
  }


def get_page_visit_daily(days: int = 30):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT strftime('%Y-%m-%d', datetime(replace(replace(visited_at, 'T', ' '), 'Z', ''), '+8 hours')) AS day,
           COUNT(*) AS visits
    FROM monitor_page_events
    WHERE datetime(replace(replace(visited_at, 'T', ' '), 'Z', ''), '+8 hours') >= datetime('now', '+8 hours', ?)
    GROUP BY day
    ORDER BY day ASC
    """,
    (f"-{max(1,int(days))} day",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_page_visit_minute(minutes: int = 180):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT strftime('%Y-%m-%d %H:%M', datetime(replace(replace(visited_at, 'T', ' '), 'Z', ''), '+8 hours')) AS minute,
           COUNT(*) AS visits
    FROM monitor_page_events
    WHERE datetime(replace(replace(visited_at, 'T', ' '), 'Z', ''), '+8 hours') >= datetime('now', '+8 hours', ?)
    GROUP BY minute
    ORDER BY minute ASC
    """,
    (f"-{max(1,int(minutes))} minute",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_page_visit_by_path(days: int = 30, limit: int = 50):
  conn = get_conn()
  if int(days or 0) <= 0:
    rows = conn.execute(
      """
      SELECT path, COUNT(*) AS visits
      FROM monitor_page_events
      GROUP BY path
      ORDER BY visits DESC
      LIMIT ?
      """,
      (max(1, int(limit)),),
    ).fetchall()
  else:
    rows = conn.execute(
      """
      SELECT path, COUNT(*) AS visits
      FROM monitor_page_events
      WHERE datetime(replace(replace(visited_at, 'T', ' '), 'Z', ''), '+8 hours') >= datetime('now', '+8 hours', ?)
      GROUP BY path
      ORDER BY visits DESC
      LIMIT ?
      """,
      (f"-{max(1,int(days))} day", max(1, int(limit))),
    ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_token_usage_daily(days: int = 30):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT strftime('%Y-%m-%d', datetime(replace(replace(logged_at, 'T', ' '), 'Z', ''), '+8 hours')) AS day,
           SUM(input_tokens) AS input_tokens,
           SUM(output_tokens) AS output_tokens,
           SUM(total_tokens) AS total_tokens
    FROM monitor_token_usage
    WHERE datetime(replace(replace(logged_at, 'T', ' '), 'Z', ''), '+8 hours') >= datetime('now', '+8 hours', ?)
    GROUP BY day
    ORDER BY day ASC
    """,
    (f"-{max(1,int(days))} day",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_token_usage_minute(minutes: int = 180):
  conn = get_conn()
  rows = conn.execute(
    """
    SELECT strftime('%Y-%m-%d %H:%M', datetime(replace(replace(logged_at, 'T', ' '), 'Z', ''), '+8 hours')) AS minute,
           SUM(input_tokens) AS input_tokens,
           SUM(output_tokens) AS output_tokens,
           SUM(total_tokens) AS total_tokens
    FROM monitor_token_usage
    WHERE datetime(replace(replace(logged_at, 'T', ' '), 'Z', ''), '+8 hours') >= datetime('now', '+8 hours', ?)
    GROUP BY minute
    ORDER BY minute ASC
    """,
    (f"-{max(1,int(minutes))} minute",),
  ).fetchall()
  conn.close()
  return [dict(r) for r in rows]


def get_monitor_totals_all_time():
  conn = get_conn()
  page_row = conn.execute(
    """
    SELECT COUNT(*) AS page_visits, MIN(visited_at) AS first_page_visit
    FROM monitor_page_events
    """
  ).fetchone()
  token_row = conn.execute(
    """
    SELECT
      COALESCE(SUM(input_tokens), 0) AS input_tokens,
      COALESCE(SUM(output_tokens), 0) AS output_tokens,
      COALESCE(SUM(total_tokens), 0) AS total_tokens,
      MIN(logged_at) AS first_token_log
    FROM monitor_token_usage
    """
  ).fetchone()
  conn.close()
  return {
    "pageVisits": int((page_row["page_visits"] if page_row else 0) or 0),
    "inputTokens": int((token_row["input_tokens"] if token_row else 0) or 0),
    "outputTokens": int((token_row["output_tokens"] if token_row else 0) or 0),
    "totalTokens": int((token_row["total_tokens"] if token_row else 0) or 0),
    "firstPageVisit": str((page_row["first_page_visit"] if page_row else "") or ""),
    "firstTokenLog": str((token_row["first_token_log"] if token_row else "") or ""),
  }


def save_polymarket_top_trader_snapshot(
  run_tag: str,
  items,
  order_by: str = "PNL",
  time_period: str = "ALL",
  fetched_at: str = "",
):
  now = now_iso()
  rtag = str(run_tag or "").strip() or now
  ts = str(fetched_at or "").strip() or now
  rows = items if isinstance(items, list) else []
  conn = get_conn()
  conn.execute("DELETE FROM polymarket_top_trader_snapshots WHERE run_tag = ?", (rtag,))
  for idx, item in enumerate(rows, start=1):
    if not isinstance(item, dict):
      continue
    wallet = str(item.get("wallet") or item.get("proxyWallet") or "").strip()
    if not wallet:
      continue
    rank_num = item.get("rank")
    try:
      rank_num = int(rank_num)
    except Exception:
      rank_num = idx
    conn.execute(
      """
      INSERT INTO polymarket_top_trader_snapshots
      (run_tag, rank_num, wallet, username, pnl, vol, order_by, time_period, fetched_at, summary_json, data_sources_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      (
        rtag,
        rank_num,
        wallet,
        str(item.get("username") or item.get("userName") or ""),
        float(item.get("pnl") or 0.0),
        float(item.get("vol") or 0.0),
        str(order_by or "PNL").upper(),
        str(time_period or "ALL").upper(),
        ts,
        json.dumps(item.get("summary") or {}, ensure_ascii=False),
        json.dumps(item.get("data_sources") or {}, ensure_ascii=False),
        now,
      ),
    )
  conn.execute(
    """
    DELETE FROM polymarket_top_trader_snapshots
    WHERE run_tag NOT IN (
      SELECT run_tag
      FROM polymarket_top_trader_snapshots
      GROUP BY run_tag
      ORDER BY MAX(fetched_at) DESC
      LIMIT 72
    )
    """
  )
  conn.commit()
  conn.close()
  return {"ok": True, "run_tag": rtag, "count": len(rows), "fetched_at": ts}


def get_latest_polymarket_top_trader_snapshot(limit: int = 10):
  lim = max(1, min(int(limit or 10), 50))
  conn = get_conn()
  row = conn.execute(
    """
    SELECT run_tag, MAX(fetched_at) AS fetched_at
    FROM polymarket_top_trader_snapshots
    GROUP BY run_tag
    ORDER BY fetched_at DESC
    LIMIT 1
    """
  ).fetchone()
  if not row:
    conn.close()
    return {"ok": True, "run_tag": "", "fetched_at": "", "items": []}
  run_tag = str(row["run_tag"] or "")
  fetched_at = str(row["fetched_at"] or "")
  rows = conn.execute(
    """
    SELECT rank_num, wallet, username, pnl, vol, order_by, time_period, fetched_at, summary_json, data_sources_json
    FROM polymarket_top_trader_snapshots
    WHERE run_tag = ?
    ORDER BY rank_num ASC
    LIMIT ?
    """,
    (run_tag, lim),
  ).fetchall()
  conn.close()
  items = []
  for r in rows:
    try:
      summary = json.loads(r["summary_json"] or "{}")
    except Exception:
      summary = {}
    try:
      data_sources = json.loads(r["data_sources_json"] or "{}")
    except Exception:
      data_sources = {}
    items.append(
      {
        "rank": int(r["rank_num"] or 0),
        "wallet": str(r["wallet"] or ""),
        "username": str(r["username"] or ""),
        "pnl": float(r["pnl"] or 0.0),
        "vol": float(r["vol"] or 0.0),
        "order_by": str(r["order_by"] or ""),
        "time_period": str(r["time_period"] or ""),
        "fetched_at": str(r["fetched_at"] or ""),
        "summary": summary,
        "data_sources": data_sources,
      }
    )
  return {"ok": True, "run_tag": run_tag, "fetched_at": fetched_at, "items": items}
