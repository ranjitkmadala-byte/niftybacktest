from __future__ import annotations
import os
from datetime import date
from zoneinfo import ZoneInfo
import psycopg
from psycopg.rows import dict_row

IST=ZoneInfo("Asia/Kolkata")
DB=(os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
DAY=date.fromisoformat(os.getenv("BACKFILL_DATE","2026-09-11"))
SYMBOL=os.getenv("SYMBOL","NIFTY").upper()

OI_THR=0.05
PRICE_THR=0.05
DELTA_THR=30.0
IMB_THR=25.0

def db(): return psycopg.connect(DB,row_factory=dict_row,connect_timeout=20)

SQL="""
WITH e0 AS (
 SELECT
   trading_date,symbol,ts,spot,future,future_oi,pcr,
   spot_change_pct_t0 AS session_price_pct,
   100.0*(future/NULLIF(LAG(future) OVER (PARTITION BY symbol ORDER BY ts),0)-1) AS price_3m_pct,
   100.0*(future_oi/NULLIF(LAG(future_oi) OVER (PARTITION BY symbol ORDER BY ts),0)-1) AS oi_3m_pct,
   pcr-LAG(pcr) OVER (PARTITION BY symbol ORDER BY ts) AS pcr_change_3m
 FROM public.index_engine_snapshots
 WHERE trading_date=%s AND symbol=%s
),
e AS (
 SELECT *,
   SUM(COALESCE(pcr_change_3m,0)) OVER (
     PARTITION BY symbol ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
   ) AS pcr_trend_9m,
   SUM(CASE WHEN pcr_change_3m>0 THEN 1 ELSE 0 END) OVER (
     PARTITION BY symbol ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
   ) AS pcr_up_3,
   SUM(CASE WHEN pcr_change_3m<0 THEN 1 ELSE 0 END) OVER (
     PARTITION BY symbol ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
   ) AS pcr_down_3
 FROM e0
)
SELECT e.*,
       a.delta_pct AS trade_delta_pct,
       a.total_qty_imbalance AS qty_imbalance_pct
FROM e
LEFT JOIN LATERAL (
 SELECT delta_pct,total_qty_imbalance
 FROM public.index_futures_aggression_snapshots a
 WHERE a.trading_date=e.trading_date
   AND a.symbol=e.symbol
   AND a.ts<=e.ts
   AND a.ts>e.ts-INTERVAL '3 minutes 30 seconds'
 ORDER BY a.ts DESC
 LIMIT 1
) a ON TRUE
ORDER BY e.ts
"""

def f(v):
    try:return float(v) if v is not None else None
    except:return None

def fmt(v, digits=4):
    v=f(v)
    return "N/A" if v is None else f"{v:.{digits}f}"

def main():
    if not DB: raise RuntimeError("NEON_DATABASE_URL required")
    with db() as c:
        with c.cursor() as x:
            x.execute(SQL,(DAY,SYMBOL))
            data=[dict(r) for r in x.fetchall()]
    if not data: raise RuntimeError("No source rows")

    px_hist=[]
    oi_hist=[]
    state_hist=[]
    scored=[]

    for r in data:
        px=f(r["price_3m_pct"]); oi=f(r["oi_3m_pct"])
        sess=f(r["session_price_pct"]); td=f(r["trade_delta_pct"]); imb=f(r["qty_imbalance_pct"])
        ptr=f(r["pcr_trend_9m"])

        px_hist.append(px)
        oi_hist.append(oi)

        # Price persistence /2
        recent=[x for x in px_hist[-3:] if x is not None]
        up=sum(x>PRICE_THR for x in recent); dn=sum(x<-PRICE_THR for x in recent)
        bull_price=2 if len(recent)==3 and up==3 else 1 if up>=2 else 0
        bear_price=2 if len(recent)==3 and dn==3 else 1 if dn>=2 else 0

        # Current futures state
        if px is not None and oi is not None:
            if px>0 and oi>0: fs="LONG_BUILDUP"
            elif px<0 and oi>0: fs="SHORT_BUILDUP"
            elif px>0 and oi<0: fs="SHORT_COVERING"
            elif px<0 and oi<0: fs="LONG_UNWINDING"
            else: fs="MIXED"
        else: fs="UNKNOWN"
        state_hist.append(fs)

        streak=1
        for z in reversed(state_hist[:-1]):
            if z==fs: streak+=1
            else: break

        # OI /2: 1 for current >= .05, second for 3 consecutive >= .05
        sign=1 if sess is not None and sess>0 else -1 if sess is not None and sess<0 else 0
        recent_oi=[x for x in oi_hist[-3:] if x is not None]
        fresh = oi is not None and oi>=OI_THR
        persistent = len(recent_oi)==3 and all(x>=OI_THR for x in recent_oi)
        bull_oi=(1 if fresh and sign>0 else 0)+(1 if persistent and sign>0 else 0)
        bear_oi=(1 if fresh and sign<0 else 0)+(1 if persistent and sign<0 else 0)

        # State persistence /2 only for directional buildup.
        stpts=2 if streak>=3 else 1 if streak>=2 else 0
        bull_state=stpts if fs=="LONG_BUILDUP" else 0
        bear_state=stpts if fs=="SHORT_BUILDUP" else 0

        # Historical true traded-value 3m flow unavailable in old Sep-11 collector.
        bull_flow=bear_flow=0

        # PCR /1
        bull_pcr=1 if ptr is not None and ptr>0 and int(r["pcr_up_3"] or 0)>=2 else 0
        bear_pcr=1 if ptr is not None and ptr<0 and int(r["pcr_down_3"] or 0)>=2 else 0

        # Aggression /1
        bull_ag=1 if td is not None and td>=DELTA_THR else 0
        bear_ag=1 if td is not None and td<=-DELTA_THR else 0

        # Imbalance /0.5
        bull_imb=.5 if imb is not None and imb>=IMB_THR else 0
        bear_imb=.5 if imb is not None and imb<=-IMB_THR else 0

        bull=bull_price+bull_oi+bull_state+bull_pcr+bull_ag+bull_imb
        bear=bear_price+bear_oi+bear_state+bear_pcr+bear_ag+bear_imb
        score=max(bull,bear)
        direction="LONG" if bull>bear else "SHORT" if bear>bull else "MIXED"

        scored.append({
          **r,"futures_state":fs,"state_streak":streak,
          "bull_price":bull_price,"bear_price":bear_price,
          "bull_oi":bull_oi,"bear_oi":bear_oi,
          "bull_state":bull_state,"bear_state":bear_state,
          "bull_pcr":bull_pcr,"bear_pcr":bear_pcr,
          "bull_ag":bull_ag,"bear_ag":bear_ag,
          "bull_imb":bull_imb,"bear_imb":bear_imb,
          "bull":bull,"bear":bear,"score":score,"direction":direction
        })

    print(f"COMPLETE rows={len(scored)} | historical available max=8.5 (money flow unavailable)")
    peak=sorted(scored,key=lambda r:(-r["score"],r["ts"]))[0]
    print(f"PEAK {peak['score']}/8.5 | {peak['direction']} | {peak['ts'].astimezone(IST):%H:%M} IST")
    print("PEAK COMPONENTS",
          f"price L/S={peak['bull_price']}/{peak['bear_price']}",
          f"oi L/S={peak['bull_oi']}/{peak['bear_oi']}",
          f"state={peak['futures_state']} x{peak['state_streak']} L/S={peak['bull_state']}/{peak['bear_state']}",
          f"pcr L/S={peak['bull_pcr']}/{peak['bear_pcr']}",
          f"agg L/S={peak['bull_ag']}/{peak['bear_ag']}",
          f"imb L/S={peak['bull_imb']}/{peak['bear_imb']}")

    for target in ("11:18","11:21","11:24","11:27","11:30"):
        hit=next((r for r in scored if r["ts"].astimezone(IST).strftime("%H:%M")==target),None)
        if not hit:
            print(target,"NO ROW"); continue
        print(
          f"{target} | px3={fmt(hit['price_3m_pct'])}% oi3={fmt(hit['oi_3m_pct'])}% "
          f"session={fmt(hit['session_price_pct'])}% | {hit['futures_state']} x{hit['state_streak']} | "
          f"price L/S={hit['bull_price']}/{hit['bear_price']} "
          f"oi L/S={hit['bull_oi']}/{hit['bear_oi']} "
          f"state L/S={hit['bull_state']}/{hit['bear_state']} "
          f"pcr L/S={hit['bull_pcr']}/{hit['bear_pcr']} "
          f"agg L/S={hit['bull_ag']}/{hit['bear_ag']} "
          f"imb L/S={hit['bull_imb']}/{hit['bear_imb']} "
          f"=> bull={hit['bull']} bear={hit['bear']} score={hit['score']} {hit['direction']}"
        )

    for threshold in (4,5,6,7,8):
        hit=next((r for r in scored if r["score"]>=threshold),None)
        print(
          f"FIRST >= {threshold}: {hit['ts'].astimezone(IST):%H:%M} IST | "
          f"{hit['score']} | {hit['direction']}"
          if hit else f"FIRST >= {threshold}: NONE"
        )

if __name__=="__main__": main()
