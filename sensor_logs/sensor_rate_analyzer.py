#!/usr/bin/env python3
"""
sensor_rate_analyzer.py  (improved)

- Auto-detect or explicitly select timestamp column (use FILE::COL to avoid 'C:' issues).
- Forces timestamp column to numeric, drops NaNs before computing deltas.
- Smarter unit inference (ns / ms / s) using both value magnitude and median diff.
- Prints summary to console and writes sensor_rate_summary.csv.

USAGE
-----
# Autodetect
python sensor_rate_analyzer.py C:\path\run_*_acc.csv C:\path\run_*_la.csv

# Explicit (recommended)
python sensor_rate_analyzer.py C:\path\run_acc.csv::accTsNs C:\path\run_la.csv::laTsNs C:\path\run_mag.csv::magTsNs C:\path\run_rotvec.csv::rvTsNs
"""
import sys, os, glob, math
import pandas as pd

PREFERRED_TS_SUFFIXES = ["TsNs", "TsNS", "TimestampNs", "timestamp_ns", "ts_ns", "tsNs"]
FALLBACK_TS_CANDIDATES = [
    "accTsNs","laTsNs","rvTsNs","magTsNs",
    "clockNs","ClockNs","timestamp","Timestamp","time","Time"
]

def choose_ts_col(df, explicit_ts=None):
    if explicit_ts:
        if explicit_ts in df.columns:
            return explicit_ts, None
        else:
            return None, f"explicit timestamp column '{explicit_ts}' not in columns: {list(df.columns)}"
    # Prefer known suffixes (e.g., accTsNs)
    for suf in PREFERRED_TS_SUFFIXES:
        for c in df.columns:
            if str(c).endswith(suf):
                return c, None
    # Known names
    for c in FALLBACK_TS_CANDIDATES:
        if c in df.columns:
            return c, None
    # Fallback: first numeric column
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            return c, None
    return None, "no suitable timestamp column found"

def infer_unit(ts_vals, diffs_pos):
    # First, if raw values are huge (>= 1e12), likely nanoseconds since boot
    if pd.Series(ts_vals).median() >= 1e12:
        return "ns"
    # Next, use median diff
    med = float(pd.Series(diffs_pos).median())
    if med > 1e6:  return "ns"
    if med > 1e3:  return "ms"
    return "s"

def analyze_file(path, explicit_ts=None):
    try:
        df = pd.read_csv(path)
    except Exception as e:
        return {"file": path, "error": f"read error: {e}"}

    ts_col, err = choose_ts_col(df, explicit_ts=explicit_ts)
    if ts_col is None:
        return {"file": path, "error": err}

    # Force numeric, drop NaNs
    ts_series = pd.to_numeric(df[ts_col], errors='coerce').dropna()
    if ts_series.size < 2:
        return {"file": path, "timestamp_col": ts_col, "error": "not enough numeric timestamps"}

    ts = ts_series.values
    diffs = ts[1:] - ts[:-1]

    # Count nonpositive deltas on raw numeric diffs
    nonpos = int((diffs <= 0).sum())
    diffs_pos = diffs[diffs > 0]
    if diffs_pos.size == 0:
        return {"file": path, "timestamp_col": ts_col, "error": "no positive deltas (unsorted or duplicate timestamps?)"}

    unit = infer_unit(ts, diffs_pos)

    if unit == "ns":
        dt_sec = diffs_pos / 1e9
    elif unit == "ms":
        dt_sec = diffs_pos / 1e3
    else:
        dt_sec = diffs_pos

    mean_dt_ms = dt_sec.mean() * 1000.0
    median_dt_ms = float(pd.Series(dt_sec).median() * 1000.0)
    min_dt_ms = dt_sec.min() * 1000.0
    max_dt_ms = dt_sec.max() * 1000.0
    mean_hz = (1.0 / dt_sec.mean()) if dt_sec.mean() > 0 else float('nan')

    return {
        "file": path,
        "timestamp_col": ts_col,
        "assumed_input_unit": unit,
        "count_intervals": int(dt_sec.size),
        "nonpositive_deltas": int(nonpos),
        "mean_dt_ms": mean_dt_ms,
        "median_dt_ms": median_dt_ms,
        "min_dt_ms": min_dt_ms,
        "max_dt_ms": max_dt_ms,
        "mean_freq_Hz": mean_hz,
    }

def split_file_and_col(arg):
    # Use FILE::COL; if no :: present, treat entire arg as a file glob
    if "::" in arg:
        filepart, col = arg.split("::", 1)
        return filepart, col
    return arg, None

def expand_argument(arg):
    filepart, col = split_file_and_col(arg)
    paths = glob.glob(filepart)
    if not paths:
        return [(filepart, col)]
    return [(p, col) for p in paths]

def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 1

    pairs = []
    for arg in argv[1:]:
        pairs.extend(expand_argument(arg))

    rows = [analyze_file(path, explicit_ts=col) for path, col in pairs]
    df_out = pd.DataFrame(rows)

    prefer = ["file","timestamp_col","assumed_input_unit","count_intervals","nonpositive_deltas",
              "mean_dt_ms","median_dt_ms","min_dt_ms","max_dt_ms","mean_freq_Hz","error"]
    for c in prefer:
        if c not in df_out.columns:
            df_out[c] = pd.NA
    df_out = df_out[prefer]

    with pd.option_context('display.max_colwidth', 120, 'display.width', 180):
        print(df_out.to_string(index=False))

    out_path = os.path.abspath("sensor_rate_summary.csv")
    try:
        df_out.to_csv(out_path, index=False)
        print(f"\nSaved summary to: {out_path}")
    except Exception as e:
        print(f"\nCould not save summary CSV: {e}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
