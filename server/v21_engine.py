#!/usr/bin/env python3
import math


def _clamp(v, lo, hi):
  return max(lo, min(hi, v))


def _safe(v, d=0.0):
  try:
    return float(v)
  except Exception:
    return float(d)


def _dim_score_map(snapshot):
  out = {}
  for d in (snapshot or {}).get('dimensions') or []:
    k = str(d.get('id') or '').strip()
    if not k:
      continue
    out[k] = _safe(d.get('score'))
  return out


def _indicator_map(snapshot):
  out = {}
  for item in (snapshot or {}).get('keyIndicatorsSnapshot') or []:
    k = str(item.get('label') or '').strip()
    if not k:
      continue
    out[k] = _safe(item.get('value'), None)
  return out


def _alert_level(score):
  if score >= 75:
    return 'green'
  if score >= 60:
    return 'yellow'
  if score >= 45:
    return 'orange'
  return 'red'


def build_v21_outputs(snapshot, regime=None, overlay=None):
  dims = _dim_score_map(snapshot)
  inds = _indicator_map(snapshot)
  total = _safe((snapshot or {}).get('totalScore'))

  d11 = dims.get('D11', 50)
  d09 = dims.get('D09', 50)
  d03 = dims.get('D03', 50)
  vix = _safe(inds.get('VIX'), 20)
  move = _safe(inds.get('MOVE'), 100)
  inf_5y5y = _safe(inds.get('5Y5Y通胀预期（%）'), _safe(inds.get('INF_EXP_5Y5Y'), 2.4))
  hy = _safe(inds.get('HY_OAS'), 350)
  wti = _safe(inds.get('WTI'), 80)

  if d11 >= 90 or wti >= 105:
    overlay_level = 'level_3'
    overlay_level_num = 3
  elif d11 >= 82 or wti >= 95 or vix >= 28:
    overlay_level = 'level_2'
    overlay_level_num = 2
  elif d11 >= 70 or vix >= 24:
    overlay_level = 'level_1'
    overlay_level_num = 1
  else:
    overlay_level = 'level_0'
    overlay_level_num = 0

  score_cap = None
  if overlay_level_num >= 3:
    score_cap = 45.0
  elif overlay_level_num >= 2:
    score_cap = 60.0

  normalized = total
  if score_cap is not None:
    normalized = min(normalized, score_cap)

  base_regime = str((regime or {}).get('regime_code') or 'growth_slowdown_credit_stable')
  # Hard overlay regime switch rule:
  # D11 level >= 2 and volatility + inflation expectation jointly rising.
  d09_vol_up = (d09 <= 55) or (vix >= 24) or (move >= 110)
  d03_infexp_up = (d03 >= 65) or (inf_5y5y >= 2.6) or (wti >= 95)
  forced_stagflation = overlay_level_num >= 2 and d09_vol_up and d03_infexp_up
  final_regime = 'stagflation_defensive' if forced_stagflation else base_regime

  override_applied = bool(score_cap is not None or forced_stagflation)
  source = 'overlay' if override_applied else 'regime'
  final_alert = 'red' if overlay_level_num >= 3 else ('orange' if overlay_level_num >= 2 else _alert_level(normalized))

  regime_confidence = _safe((regime or {}).get('confidence'), 0.65)
  signal_conf = _clamp(55 + overlay_level_num * 10 + (8 if override_applied else 0), 0, 95)

  if final_regime in {'stagflation_defensive', 'credit_stress_risk_off'} or final_alert in {'red', 'orange'}:
    action_direction = 'defensive'
    action_size_cap = 0.10 if final_alert == 'red' else 0.15
    hedge_preference = 'index_hedge_first'
  elif normalized >= 65 and overlay_level_num <= 1:
    action_direction = 'risk_on'
    action_size_cap = 0.20
    hedge_preference = 'light_hedge'
  else:
    action_direction = 'neutral'
    action_size_cap = 0.12
    hedge_preference = 'barbell_quality'

  if override_applied:
    topline_message = 'Overlay 已接管最终结论，背景分仅作参考。'
  else:
    topline_message = '当前以 Regime 为主导，Overlay 未触发否决。'

  layer_defs = {
    'shock': ['D11', 'D09', 'D07', 'D08'],
    'tactical': ['D03', 'D04', 'D05', 'D10'],
    'cyclical': ['D01', 'D02', 'D06', 'D12', 'D13', 'D14'],
  }
  layer_rows = []
  for layer, codes in layer_defs.items():
    vals = [dims.get(c, 50) for c in codes]
    score = round(sum(vals) / max(1, len(vals)), 2)
    if layer == 'shock' and overlay_level_num >= 2:
      layer_regime = 'energy_shock_risk'
    elif layer == 'tactical' and d03 >= 70:
      layer_regime = 'inflation_pressure'
    elif layer == 'cyclical' and dims.get('D02', 50) < 50:
      layer_regime = 'growth_softening'
    else:
      layer_regime = 'mixed'
    conf = round(_clamp(50 + abs(score - 50) * 0.8 + overlay_level_num * 4, 45, 92), 1)
    layer_rows.append({
      'layer': layer,
      'layer_score': score,
      'layer_regime': layer_regime,
      'layer_confidence': conf,
      'key_drivers': [f'{codes[0]} score {round(vals[0],1)}'],
      'key_risks': [f'VIX {round(vix,2)}', f'HY_OAS {round(hy,1)}'],
    })

  dim_rows = []
  for code, static_score in sorted(dims.items()):
    if code in layer_defs['shock']:
      layer = 'shock'
    elif code in layer_defs['tactical']:
      layer = 'tactical'
    else:
      layer = 'cyclical'
    momentum = _clamp(50 + (static_score - 50) * 0.7, 0, 100)
    resonance = 0.0
    if overlay_level_num >= 2 and code in {'D11', 'D09', 'D07', 'D03'}:
      resonance = 18.0
    tail = 0.0
    if static_score < 45:
      tail += 8.0
    if overlay_level_num >= 3 and code in {'D11', 'D09'}:
      tail += 12.0
    pre = 0.40 * static_score + 0.35 * momentum + 0.25 * max(0.0, 100.0 - resonance)
    final = _clamp(pre - tail, 0, 100)
    if final > static_score + 2:
      direction = 'improving'
    elif final < static_score - 2:
      direction = 'weakening'
    else:
      direction = 'flat'
    dim_rows.append({
      'dimension_code': code,
      'layer': layer,
      'static_level_score': round(static_score, 2),
      'momentum_score': round(momentum, 2),
      'resonance_penalty': round(resonance, 2),
      'tail_penalty': round(tail, 2),
      'pre_overlay_score': round(pre, 2),
      'final_dimension_score': round(final, 2),
      'signal_direction': direction,
      'alert_color': _alert_level(final),
    })

  overlay_decision = {
    'overlay_type': 'geopolitical',
    'overlay_level': overlay_level,
    'override_applied': override_applied,
    'score_cap': score_cap,
    'regime_override': final_regime if forced_stagflation else '',
    'alert_override': final_alert if override_applied else '',
    'rationale': '能源冲击与风险偏好共同触发防御约束。' if override_applied else '未达到 Overlay 否决阈值。',
    'triggered_signals': [
      f'D11={round(d11,2)}',
      f'WTI={round(wti,2)}',
      f'VIX={round(vix,2)}',
      f'MOVE={round(move,2)}',
      f'INF5Y5Y={round(inf_5y5y,3)}',
      f'D03={round(d03,2)}',
      f'D09={round(d09,2)}',
      f'D09_VOL_UP={1 if d09_vol_up else 0}',
      f'D03_INFEXP_UP={1 if d03_infexp_up else 0}',
      f'FORCED_STAGFLATION={1 if forced_stagflation else 0}',
    ],
  }

  geo = {
    'conflict_intensity_score': round(_clamp(d11, 0, 100), 2),
    'supply_disruption_score': round(_clamp((wti - 70) * 1.2, 0, 100), 2),
    'shipping_insurance_score': round(_clamp(30 + overlay_level_num * 20, 0, 100), 2),
    'energy_microstructure_score': round(_clamp((wti - 75) * 1.1, 0, 100), 2),
    'macro_transmission_score': round(_clamp((vix - 15) * 4 + max(0, (hy - 300) / 3), 0, 100), 2),
    'overlay_level': overlay_level,
    'brent_price': round(wti + 3.5, 2),
    'brent_5d_change_pct': round(_clamp((wti - 90) * 0.9, -20, 20), 2),
    'hormuz_status': 'stressed' if overlay_level_num >= 2 else 'normal',
    'conclusion': '能源与地缘冲击已进入需防御管理阶段。' if overlay_level_num >= 2 else '地缘风险可控，维持观察。',
  }

  run_payload = {
    'score_background': round(total, 2),
    'normalized_score': round(normalized, 2),
    'total_score': round(total, 2),
    'final_regime': final_regime,
    'regime_confidence': round(regime_confidence * 100 if regime_confidence <= 1.2 else regime_confidence, 2),
    'overlay_level': overlay_level,
    'overlay_override_applied': override_applied,
    'score_cap_applied': score_cap,
    'primary_decision_source': source,
    'topline_message': topline_message,
    'status': final_alert,
    'payload': {
      'decision_priority': 'overlay_first' if override_applied else 'regime_first',
      'action_direction': action_direction,
      'action_size_cap': action_size_cap,
      'hedge_preference': hedge_preference,
      'signal_confidence_score': signal_conf,
      'layer_snapshot': layer_rows,
    },
  }

  daily_analysis = {
    'score_background': run_payload['score_background'],
    'final_regime': final_regime,
    'final_alert_level': final_alert,
    'decision_priority': run_payload['payload']['decision_priority'],
    'signal_confidence_score': signal_conf,
    'overlay_summary': overlay_decision.get('rationale') or '',
    'action_size_cap': action_size_cap,
    'hedge_preference': hedge_preference,
    'payload': run_payload['payload'],
  }

  return {
    'run': run_payload,
    'layers': layer_rows,
    'dimension_scores': dim_rows,
    'overlay_decision': overlay_decision,
    'geopolitical_overlay': geo,
    'daily_analysis': daily_analysis,
  }
