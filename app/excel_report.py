"""Professional 6-sheet Excel Daily Trading Report generator for Trader Tracking System (TTS).

Workbook filename: TTS_Daily_Trading_Report_YYYY-MM-DD.xlsx
Sheets:
  1. Executive Summary
  2. Trader Performance
  3. Account Financial Summary
  4. Trade Details
  5. Red Flag Details
  6. Risk Analysis

Strictly preserves existing TTS financial definitions and NEVER invents unavailable broker data.
"""
from __future__ import annotations

import io
import os
import tempfile
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

_NAVY_FILL = PatternFill("solid", fgColor="0F172A")
_SUB_FILL = PatternFill("solid", fgColor="F1F5F9")
_TOTAL_FILL = PatternFill("solid", fgColor="E2E8F0")
_HIGH_FILL = PatternFill("solid", fgColor="FDECEA")
_MED_FILL = PatternFill("solid", fgColor="FEF3C7")
_LOW_FILL = PatternFill("solid", fgColor="F1F5F9")
_PROVISIONAL_FILL = PatternFill("solid", fgColor="FEF3C7")
_FINAL_FILL = PatternFill("solid", fgColor="E7F6EE")

_TITLE_FONT = Font(name="Calibri", size=14, bold=True, color="0F172A")
_SUBTITLE_FONT = Font(name="Calibri", size=10, italic=True, color="475569")
_HDR_FONT = Font(name="Calibri", size=10, bold=True, color="FFFFFF")
_BOLD_FONT = Font(name="Calibri", size=10, bold=True, color="0F172A")
_BODY_FONT = Font(name="Calibri", size=10, color="111827")
_POS_FONT = Font(name="Calibri", size=10, bold=True, color="0F9D58")
_NEG_FONT = Font(name="Calibri", size=10, bold=True, color="D93025")
_HIGH_FONT = Font(name="Calibri", size=10, bold=True, color="D93025")
_MED_FONT = Font(name="Calibri", size=10, bold=True, color="B45309")

_THIN_BORDER = Border(
    left=Side(style="thin", color="E2E8F0"),
    right=Side(style="thin", color="E2E8F0"),
    top=Side(style="thin", color="E2E8F0"),
    bottom=Side(style="thin", color="E2E8F0"),
)

_INR_FMT = '₹#,##0.00;[Red]-₹#,##0.00;"₹0.00"'
_PCT_FMT = "0.0%"


def _safe_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        f = float(val)
        return f if (f == f) else None
    except (TypeError, ValueError):
        return None


def _style_header_row(ws, row_idx: int, num_cols: int):
    ws.row_dimensions[row_idx].height = 22
    for c in range(1, num_cols + 1):
        cell = ws.cell(row=row_idx, column=c)
        cell.fill = _NAVY_FILL
        cell.font = _HDR_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = _THIN_BORDER


def _style_title(ws, title: str, subtitle: str, num_cols: int):
    end_col = get_column_letter(max(2, num_cols))
    ws.merge_cells(f"A1:{end_col}1")
    ws["A1"] = title
    ws["A1"].font = _TITLE_FONT
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 24

    ws.merge_cells(f"A2:{end_col}2")
    ws["A2"] = subtitle
    ws["A2"].font = _SUBTITLE_FONT
    ws["A2"].alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 18


def _auto_fit_columns(ws, min_width: int = 12, max_width: int = 44):
    for col in ws.columns:
        col_letter = get_column_letter(col[0].column)
        max_len = 0
        for cell in col:
            if cell.row in (1, 2):
                continue
            val = cell.value
            if val is None:
                continue
            s = str(val)
            if len(s) > max_len:
                max_len = len(s)
        ws.column_dimensions[col_letter].width = max(min_width, min(max_width, max_len + 4))


def _apply_pnl_cell(cell, val: Optional[float]):
    if val is None:
        cell.value = "Unavailable"
        cell.font = _SUBTITLE_FONT
        cell.alignment = Alignment(horizontal="right")
    else:
        cell.value = round(val, 2)
        cell.number_format = _INR_FMT
        cell.font = _POS_FONT if val > 0 else (_NEG_FONT if val < 0 else _BODY_FONT)
        cell.alignment = Alignment(horizontal="right")
    cell.border = _THIN_BORDER


def _apply_currency_cell(cell, val: Optional[float]):
    if val is None:
        cell.value = "Unavailable"
        cell.font = _SUBTITLE_FONT
        cell.alignment = Alignment(horizontal="right")
    else:
        cell.value = round(val, 2)
        cell.number_format = _INR_FMT
        cell.font = _BODY_FONT
        cell.alignment = Alignment(horizontal="right")
    cell.border = _THIN_BORDER


def generate_daily_excel_bytes(report_data: Dict[str, Any]) -> Tuple[str, bytes]:
    """Build the 6-sheet Excel daily report and return (filename, xlsx_bytes).

    Uses a private temporary file if needed and ensures clean in-memory bytes with zero
    public URLs or lingering temp files.
    """
    trading_date = str(report_data.get("trading_date") or "unknown")
    report_status = str(report_data.get("report_status") or "FINAL").upper()
    generated_ist = str(report_data.get("generated_ist") or "")
    sync_note = str(report_data.get("sync_note") or "")
    summary = report_data.get("summary") or {}
    trader_rows = report_data.get("trader_performance") or []
    account_rows = report_data.get("account_financials") or []
    trade_rows = report_data.get("trade_details") or []
    flag_rows = report_data.get("red_flags") or []

    filename = f"TTS_Daily_Trading_Report_{trading_date}.xlsx"
    wb = Workbook()

    # =========================================================================
    # SHEET 1 — EXECUTIVE SUMMARY
    # =========================================================================
    ws1 = wb.active
    ws1.title = "Executive Summary"
    ws1.views.sheetView[0].showGridLines = True
    subtitle_1 = f"Trading Date: {trading_date}  |  Status: {report_status}  |  Generated: {generated_ist}"
    _style_title(ws1, "Trader Tracking System — Daily Executive Summary", subtitle_1, 3)

    ws1.append([])
    ws1.append(["Metric", "Value", "Notes"])
    _style_header_row(ws1, 4, 3)
    ws1.freeze_panes = "A5"

    exec_rows = [
        ("Trading Date", trading_date, "Asia/Kolkata (IST)", "text"),
        ("Report Status", report_status, sync_note or ("Finalized EOD data" if report_status == "FINAL" else "Provisional — final broker sync incomplete"), "status"),
        ("Realized P&L", _safe_float(summary.get("realized_pnl")), "Booked P&L (existing TTS definition)", "pnl"),
        ("Unrealized MTM", _safe_float(summary.get("unrealized_mtm")), "Open mark-to-market exposure", "pnl"),
        ("Overall P&L", _safe_float(summary.get("overall_pnl")), "Realized P&L + Unrealized MTM", "pnl"),
        ("Total Available Funds", _safe_float(summary.get("available_funds")), summary.get("funds_note") or "Zebu Limits API", "currency"),
        ("Total Utilized Funds", _safe_float(summary.get("utilized_funds")), summary.get("funds_note") or "Zebu Limits API", "currency"),
        ("Total Trades", int(summary.get("total_trades") or 0), "Trades recorded for trading date", "int"),
        ("Active Traders", int(summary.get("active_traders") or 0), "Traders with activity / linked accounts", "int"),
        ("Rule-Follow Percentage", _safe_float(summary.get("rule_follow_pct")), "Clean trades / total trades", "pct"),
        ("Red Flags — High", int(summary.get("high_flags") or 0), "High-severity rule violations", "int_high"),
        ("Red Flags — Medium", int(summary.get("medium_flags") or 0), "Medium-severity rule violations", "int"),
        ("Red Flags — Low", int(summary.get("low_flags") or 0), "Low-severity rule violations", "int"),
        ("Red Flags — Total", int(summary.get("total_flags") or 0), f"{int(summary.get('traders_with_flags') or 0)} trader(s) flagged", "int"),
    ]
    for idx, (label, val, note, kind) in enumerate(exec_rows, start=5):
        c1 = ws1.cell(row=idx, column=1, value=label)
        c2 = ws1.cell(row=idx, column=2)
        c3 = ws1.cell(row=idx, column=3, value=note)
        c1.font = _BOLD_FONT
        c1.border = _THIN_BORDER
        c3.font = _SUBTITLE_FONT
        c3.border = _THIN_BORDER
        if kind == "pnl":
            _apply_pnl_cell(c2, val)
        elif kind == "currency":
            _apply_currency_cell(c2, val)
        elif kind == "pct":
            if val is None:
                c2.value = "N/A"
                c2.font = _SUBTITLE_FONT
            else:
                c2.value = val / 100.0
                c2.number_format = _PCT_FMT
                c2.font = _POS_FONT if val >= 95 else _BOLD_FONT
            c2.alignment = Alignment(horizontal="right")
            c2.border = _THIN_BORDER
        elif kind == "status":
            c2.value = val
            c2.font = _POS_FONT if val == "FINAL" else _MED_FONT
            c2.fill = _FINAL_FILL if val == "FINAL" else _PROVISIONAL_FILL
            c2.alignment = Alignment(horizontal="center")
            c2.border = _THIN_BORDER
        elif kind == "int_high":
            c2.value = val
            c2.font = _HIGH_FONT if (val or 0) > 0 else _BODY_FONT
            c2.alignment = Alignment(horizontal="right")
            c2.border = _THIN_BORDER
        elif kind == "int":
            c2.value = val
            c2.font = _BODY_FONT
            c2.alignment = Alignment(horizontal="right")
            c2.border = _THIN_BORDER
        else:
            c2.value = val
            c2.font = _BODY_FONT
            c2.alignment = Alignment(horizontal="right")
            c2.border = _THIN_BORDER

    _auto_fit_columns(ws1, min_width=18, max_width=48)

    # =========================================================================
    # SHEET 2 — TRADER PERFORMANCE
    # =========================================================================
    ws2 = wb.create_sheet("Trader Performance")
    _style_title(ws2, "Trader Performance", f"Trading Date: {trading_date}  |  Status: {report_status}", 7)
    ws2.append([])
    hdr2 = ["Trader Name", "Trader ID", "Broker Account", "Realized P&L", "Unrealized MTM", "Total P&L", "Trade Count", "Rule-Follow %"]
    ws2.append(hdr2)
    _style_header_row(ws2, 4, len(hdr2))
    ws2.freeze_panes = "A5"

    tot_r2 = 0.0
    tot_u2 = 0.0
    tot_t2 = 0.0
    tot_cnt2 = 0
    for r_i, tr in enumerate(trader_rows, start=5):
        rp = _safe_float(tr.get("realized_pnl"))
        up = _safe_float(tr.get("unrealized_mtm"))
        tp = _safe_float(tr.get("total_pnl"))
        cnt = int(tr.get("trade_count") or 0)
        rf = _safe_float(tr.get("rule_follow_pct"))
        if rp is not None:
            tot_r2 += rp
        if up is not None:
            tot_u2 += up
        if tp is not None:
            tot_t2 += tp
        tot_cnt2 += cnt

        ws2.cell(row=r_i, column=1, value=tr.get("trader_name") or tr.get("trader_id") or "—").border = _THIN_BORDER
        ws2.cell(row=r_i, column=2, value=tr.get("trader_id") or "—").border = _THIN_BORDER
        ws2.cell(row=r_i, column=3, value=tr.get("broker_account") or "—").border = _THIN_BORDER
        _apply_pnl_cell(ws2.cell(row=r_i, column=4), rp)
        _apply_pnl_cell(ws2.cell(row=r_i, column=5), up)
        _apply_pnl_cell(ws2.cell(row=r_i, column=6), tp)
        c_cnt = ws2.cell(row=r_i, column=7, value=cnt)
        c_cnt.alignment = Alignment(horizontal="right")
        c_cnt.border = _THIN_BORDER
        c_rf = ws2.cell(row=r_i, column=8)
        if rf is None:
            c_rf.value = "N/A"
            c_rf.font = _SUBTITLE_FONT
        else:
            c_rf.value = rf / 100.0
            c_rf.number_format = _PCT_FMT
            c_rf.font = _POS_FONT if rf >= 95 else _BODY_FONT
        c_rf.alignment = Alignment(horizontal="right")
        c_rf.border = _THIN_BORDER

    tot_row2 = len(trader_rows) + 5
    ws2.cell(row=tot_row2, column=1, value="TOTAL").font = _BOLD_FONT
    _apply_pnl_cell(ws2.cell(row=tot_row2, column=4), tot_r2)
    _apply_pnl_cell(ws2.cell(row=tot_row2, column=5), tot_u2)
    _apply_pnl_cell(ws2.cell(row=tot_row2, column=6), tot_t2)
    c_tc = ws2.cell(row=tot_row2, column=7, value=tot_cnt2)
    c_tc.font = _BOLD_FONT
    c_tc.alignment = Alignment(horizontal="right")
    for c in range(1, len(hdr2) + 1):
        ws2.cell(row=tot_row2, column=c).fill = _TOTAL_FILL
        ws2.cell(row=tot_row2, column=c).border = _THIN_BORDER
    ws2.auto_filter.ref = f"A4:{get_column_letter(len(hdr2))}{max(4, tot_row2 - 1)}"
    _auto_fit_columns(ws2)

    # =========================================================================
    # SHEET 3 — ACCOUNT FINANCIAL SUMMARY
    # =========================================================================
    ws3 = wb.create_sheet("Account Financial Summary")
    _style_title(ws3, "Account Financial Summary", f"Trading Date: {trading_date}  |  Status: {report_status}", 7)
    ws3.append([])
    hdr3 = ["Broker Account", "Account Name", "Zebu UID", "Available Funds", "Utilized Funds", "Realized P&L", "Unrealized MTM", "Data Status"]
    ws3.append(hdr3)
    _style_header_row(ws3, 4, len(hdr3))
    ws3.freeze_panes = "A5"

    tot_av, tot_ut, tot_rp3, tot_ur3 = 0.0, 0.0, 0.0, 0.0
    any_av, any_ut = False, False
    for r_i, acc in enumerate(account_rows, start=5):
        av = _safe_float(acc.get("available_funds"))
        ut = _safe_float(acc.get("utilized_funds"))
        rp = _safe_float(acc.get("realized_pnl"))
        ur = _safe_float(acc.get("unrealized_mtm"))
        if av is not None:
            tot_av += av
            any_av = True
        if ut is not None:
            tot_ut += ut
            any_ut = True
        if rp is not None:
            tot_rp3 += rp
        if ur is not None:
            tot_ur3 += ur

        ws3.cell(row=r_i, column=1, value=acc.get("account_id") or "—").border = _THIN_BORDER
        ws3.cell(row=r_i, column=2, value=acc.get("account_name") or "—").border = _THIN_BORDER
        ws3.cell(row=r_i, column=3, value=acc.get("zebu_uid") or "—").border = _THIN_BORDER
        _apply_currency_cell(ws3.cell(row=r_i, column=4), av)
        _apply_currency_cell(ws3.cell(row=r_i, column=5), ut)
        _apply_pnl_cell(ws3.cell(row=r_i, column=6), rp)
        _apply_pnl_cell(ws3.cell(row=r_i, column=7), ur)
        ws3.cell(row=r_i, column=8, value=acc.get("status") or "—").border = _THIN_BORDER

    tot_row3 = len(account_rows) + 5
    ws3.cell(row=tot_row3, column=1, value="TOTAL").font = _BOLD_FONT
    _apply_currency_cell(ws3.cell(row=tot_row3, column=4), tot_av if any_av else None)
    _apply_currency_cell(ws3.cell(row=tot_row3, column=5), tot_ut if any_ut else None)
    _apply_pnl_cell(ws3.cell(row=tot_row3, column=6), tot_rp3)
    _apply_pnl_cell(ws3.cell(row=tot_row3, column=7), tot_ur3)
    for c in range(1, len(hdr3) + 1):
        ws3.cell(row=tot_row3, column=c).fill = _TOTAL_FILL
        ws3.cell(row=tot_row3, column=c).border = _THIN_BORDER
    ws3.auto_filter.ref = f"A4:{get_column_letter(len(hdr3))}{max(4, tot_row3 - 1)}"
    _auto_fit_columns(ws3)

    # =========================================================================
    # SHEET 4 — TRADE DETAILS
    # =========================================================================
    ws4 = wb.create_sheet("Trade Details")
    _style_title(ws4, "Trade Details", f"Trading Date: {trading_date}  |  Total Trades: {len(trade_rows)}", 14)
    ws4.append([])
    hdr4 = [
        "Entry Date", "Entry Time", "Exit Date", "Exit Time",
        "Trader", "Account", "Instrument Name", "Expiry Date",
        "Buy/Sell", "Lots", "Quantity", "Entry Price", "Exit Price", "P&L", "Product", "Status",
    ]
    ws4.append(hdr4)
    _style_header_row(ws4, 4, len(hdr4))
    ws4.freeze_panes = "A5"

    tot_pnl4 = 0.0
    for r_i, t in enumerate(trade_rows, start=5):
        pnl_v = _safe_float(t.get("pnl"))
        if pnl_v is not None:
            tot_pnl4 += pnl_v
        inst_name = t.get("instrument_name") or f"{t.get('symbol') or ''} {t.get('strike') or ''} {t.get('opt') or ''}".strip()
        exp_val = str(t.get("expiry") or "").strip() or "N/A"
        vals = [
            t.get("date") or "—",
            t.get("time") or "—",
            t.get("exitDate") or (t.get("date") if not t.get("open") else "OPEN"),
            t.get("exitTime") or ("OPEN" if t.get("open") else "—"),
            t.get("trader_name") or t.get("trader") or "—",
            t.get("account") or "—",
            inst_name,
            exp_val,
            t.get("side") or "—",
            _safe_float(t.get("lots")) or 0,
            _safe_float(t.get("qty")) or 0,
            _safe_float(t.get("entry")),
            _safe_float(t.get("exit")),
            pnl_v,
            t.get("product") or "—",
            "OPEN" if t.get("open") else "CLOSED",
        ]
        for c_i, val in enumerate(vals, start=1):
            cell = ws4.cell(row=r_i, column=c_i)
            if c_i == 14:
                _apply_pnl_cell(cell, val)
            elif c_i in (12, 13):
                if val is None:
                    cell.value = "—"
                else:
                    cell.value = round(val, 2)
                    cell.number_format = "#,##0.00"
                    cell.alignment = Alignment(horizontal="right")
                cell.border = _THIN_BORDER
            else:
                cell.value = val
                cell.font = _BODY_FONT
                if c_i in (10, 11):
                    cell.alignment = Alignment(horizontal="right")
                cell.border = _THIN_BORDER

    tot_row4 = len(trade_rows) + 5
    ws4.cell(row=tot_row4, column=1, value="TOTAL").font = _BOLD_FONT
    _apply_pnl_cell(ws4.cell(row=tot_row4, column=14), tot_pnl4)
    for c in range(1, len(hdr4) + 1):
        ws4.cell(row=tot_row4, column=c).fill = _TOTAL_FILL
        ws4.cell(row=tot_row4, column=c).border = _THIN_BORDER
    ws4.auto_filter.ref = f"A4:{get_column_letter(len(hdr4))}{max(4, tot_row4 - 1)}"
    _auto_fit_columns(ws4)

    # =========================================================================
    # SHEET 5 — RED FLAG DETAILS
    # =========================================================================
    ws5 = wb.create_sheet("Red Flag Details")
    _style_title(ws5, "Red Flag Details", f"Trading Date: {trading_date}  |  Total Flags: {len(flag_rows)}", 9)
    ws5.append([])
    hdr5 = ["Trader", "Account", "Rule Violated", "Severity", "Instrument", "Detection Time", "Observed Value", "Allowed Value", "Flag Status"]
    ws5.append(hdr5)
    _style_header_row(ws5, 4, len(hdr5))
    ws5.freeze_panes = "A5"

    for r_i, fl in enumerate(flag_rows, start=5):
        sev = str(fl.get("severity") or "Medium")
        vals = [
            fl.get("trader_name") or fl.get("trader") or "—",
            fl.get("account") or "—",
            fl.get("rule") or "—",
            sev,
            fl.get("instrument") or "—",
            fl.get("detection_time") or "—",
            fl.get("observed_value") or fl.get("did") or "—",
            fl.get("allowed_value") or fl.get("condition") or "—",
            fl.get("status") or "ACTIVE",
        ]
        for c_i, val in enumerate(vals, start=1):
            cell = ws5.cell(row=r_i, column=c_i, value=val)
            cell.font = _BODY_FONT
            cell.border = _THIN_BORDER
            if c_i == 4:
                cell.alignment = Alignment(horizontal="center")
                if sev == "High":
                    cell.fill = _HIGH_FILL
                    cell.font = _HIGH_FONT
                elif sev == "Medium":
                    cell.fill = _MED_FILL
                    cell.font = _MED_FONT
                else:
                    cell.fill = _LOW_FILL

    ws5.auto_filter.ref = f"A4:{get_column_letter(len(hdr5))}{max(4, len(flag_rows) + 4)}"
    _auto_fit_columns(ws5)

    # =========================================================================
    # SHEET 6 — RISK ANALYSIS
    # =========================================================================
    ws6 = wb.create_sheet("Risk Analysis")
    _style_title(ws6, "Risk Analysis", f"Trading Date: {trading_date}  |  Compliance Breakdown", 3)
    ws6.append([])
    ws6.append(["Risk Metric", "Value", "Detail"])
    _style_header_row(ws6, 4, 3)
    ws6.freeze_panes = "A5"

    high_n = sum(1 for x in flag_rows if str(x.get("severity")) == "High")
    med_n = sum(1 for x in flag_rows if str(x.get("severity")) == "Medium")
    low_n = sum(1 for x in flag_rows if str(x.get("severity")) == "Low")
    flagged_traders = sorted({str(x.get("trader_name") or x.get("trader") or "") for x in flag_rows if x.get("trader_name") or x.get("trader")})
    rf_pct = _safe_float(summary.get("rule_follow_pct"))

    risk_metrics = [
        ("Total Red Flags", len(flag_rows), "All severities combined"),
        ("High-Severity Flags", high_n, "Immediate alert level"),
        ("Medium-Severity Flags", med_n, "Watch level"),
        ("Low-Severity Flags", low_n, "Informational level"),
        ("Traders with Flags", len(flagged_traders), ", ".join(flagged_traders) if flagged_traders else "None"),
        ("Rule-Follow Percentage", f"{rf_pct:.1f}%" if rf_pct is not None else "N/A", "Clean trades / total trades"),
    ]
    for idx, (m_k, m_v, m_d) in enumerate(risk_metrics, start=5):
        c1 = ws6.cell(row=idx, column=1, value=m_k)
        c2 = ws6.cell(row=idx, column=2, value=m_v)
        c3 = ws6.cell(row=idx, column=3, value=m_d)
        c1.font = _BOLD_FONT
        c2.font = _HIGH_FONT if (m_k == "High-Severity Flags" and high_n > 0) else _BODY_FONT
        c2.alignment = Alignment(horizontal="right")
        c3.font = _SUBTITLE_FONT
        c1.border = c2.border = c3.border = _THIN_BORDER

    # Most Frequently Violated Rules section
    top_start = 5 + len(risk_metrics) + 2
    ws6.cell(row=top_start - 1, column=1, value="Most Frequently Violated Rules").font = _BOLD_FONT
    ws6.cell(row=top_start, column=1, value="Rule Violated")
    ws6.cell(row=top_start, column=2, value="Violation Count")
    ws6.cell(row=top_start, column=3, value="Primary Severity")
    _style_header_row(ws6, top_start, 3)

    rule_counts = Counter(str(x.get("rule") or "Unknown") for x in flag_rows)
    rule_sev: Dict[str, str] = {}
    for x in flag_rows:
        rn = str(x.get("rule") or "Unknown")
        if rn not in rule_sev or x.get("severity") == "High":
            rule_sev[rn] = str(x.get("severity") or "Medium")

    if rule_counts:
        for offset, (r_name, cnt) in enumerate(rule_counts.most_common(15), start=1):
            r_idx = top_start + offset
            ws6.cell(row=r_idx, column=1, value=r_name).border = _THIN_BORDER
            c_n = ws6.cell(row=r_idx, column=2, value=cnt)
            c_n.alignment = Alignment(horizontal="right")
            c_n.border = _THIN_BORDER
            ws6.cell(row=r_idx, column=3, value=rule_sev.get(r_name, "Medium")).border = _THIN_BORDER
    else:
        ws6.cell(row=top_start + 1, column=1, value="No rule violations recorded for this trading date.").font = _SUBTITLE_FONT

    _auto_fit_columns(ws6, min_width=18, max_width=52)

    # Save via private temporary file with immediate cleanup (ensures valid zip/xlsx structure, zero leaked files)
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(prefix="tts_rep_", suffix=".xlsx")
        os.close(fd)
        wb.save(tmp_path)
        with open(tmp_path, "rb") as f:
            data = f.read()
        return filename, data
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
