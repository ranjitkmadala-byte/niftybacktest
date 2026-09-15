from __future__ import annotations
import os
from datetime import date
from zoneinfo import ZoneInfo
import pandas as pd
import psycopg
from psycopg.rows import dict_row

IST=ZoneInfo("Asia/Kolkata")
DB=(os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
DAY=date.fromisoformat(os.getenv("BACKFILL_DATE","2026-09-11"))
SYMBOL=os.getenv("SYMBOL","NIFTY").upper()
TARGET="index_v29_backtest"

def db(): return psycopg.connect(DB,row_factory=dict_row,connect_timeout=20)
def tail_count(vals,pred):
    n=0
    for v in reversed(list(vals)):
        try: ok=pred(float(v))
        except: ok=False
        if not ok: break
        n+=1
    return n

DDL=f"""
CREATE TABLE IF NOT EXISTS public.{TARGET}(
 trading_date DATE,symbol TEXT,ts TIMESTAMPTZ,
 price_persistence_points NUMERIC,oi_confirmation_points NUMERIC,
 futures_state TEXT,futures_state_persistence INTEGER,futures_state_points NUMERIC,
 money_flow_acceleration_x NUMERIC,money_flow_points NUMERIC,
 pcr_trend_9m NUMERIC,pcr_trend_points NUMERIC,
 aggression_points NUMERIC,imbalance_points NUMERIC,
 bull_score NUMERIC,bear_score NUMERIC,score NUMERIC,direction TEXT,state TEXT,
 spot NUMERIC,session_price_pct NUMERIC,price_3m_pct NUMERIC,oi_3m_pct NUMERIC,
 cumulative_oi_pct NUMERIC,pcr NUMERIC,trade_delta_pct NUMERIC,qty_imbalance NUMERIC,
 PRIMARY KEY(trading_date,symbol,ts)
);
"""

def main():
    if not DB: raise RuntimeError("NEON_DATABASE_URL required")
    with db() as c:
        with c.cursor() as x:x.execute(DDL)
        c.commit()

    # Read with psycopg directly. PostgreSQL also returns a clean IST timestamp
    # text field so pandas never has to infer mixed timestamp formats.
    with db() as c:
        with c.cursor() as x:
            x.execute("""
              SELECT *,
                     to_char(ts AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS') AS ts_ist_text,
                     100.0*(future/NULLIF(LAG(future) OVER (ORDER BY ts),0)-1) AS sql_price_3m_pct,
                     100.0*(future_oi/NULLIF(LAG(future_oi) OVER (ORDER BY ts),0)-1) AS sql_oi_3m_pct,
                     pcr-LAG(pcr) OVER (ORDER BY ts) AS sql_pcr_change_3m
              FROM public.index_engine_snapshots
              WHERE trading_date=%s AND symbol=%s
              ORDER BY ts
            """,(DAY,SYMBOL))
            eng_rows=x.fetchall()

            x.execute("""
              SELECT *,
                     to_char(ts AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS') AS ts_ist_text
              FROM public.index_futures_aggression_snapshots
              WHERE trading_date=%s AND symbol=%s
              ORDER BY ts
            """,(DAY,SYMBOL))
            agg_rows=x.fetchall()

    eng=pd.DataFrame([dict(r) for r in eng_rows])
    agg=pd.DataFrame([dict(r) for r in agg_rows])

    print("SOURCE CHECK")
    print(f"index_engine_snapshots: {len(eng)}")
    print(f"index_futures_aggression_snapshots: {len(agg)}")

    if eng.empty:
        raise RuntimeError("No Sep-11 NIFTY rows in index_engine_snapshots")

    print(f"engine first={eng.iloc[0]['ts_ist_text']} IST | last={eng.iloc[-1]['ts_ist_text']} IST")
    if not agg.empty:
        print(f"aggression first={agg.iloc[0]['ts_ist_text']} IST | last={agg.iloc[-1]['ts_ist_text']} IST")

    # Build timezone-aware timestamps from PostgreSQL's normalized IST text.
    eng["ts"]=pd.to_datetime(
        eng["ts_ist_text"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    ).dt.tz_localize(IST)

    eng=eng[eng["ts"].notna()].sort_values("ts").reset_index(drop=True)

    if not agg.empty:
        agg["ts"]=pd.to_datetime(
            agg["ts_ist_text"],
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce"
        ).dt.tz_localize(IST)
        agg=agg[agg["ts"].notna()].sort_values("ts").reset_index(drop=True)

    print(f"timestamp-normalized engine rows: {len(eng)}")
    print(f"timestamp-normalized aggression rows: {len(agg)}")

    # derive missing 3m fields from stored snapshots
    eng["future"]=pd.to_numeric(eng.future,errors="coerce")
    eng["future_oi"]=pd.to_numeric(eng.future_oi,errors="coerce")
    eng["spot"]=pd.to_numeric(eng.spot,errors="coerce")
    eng["px3"]=pd.to_numeric(eng["sql_price_3m_pct"],errors="coerce")
    eng["oi3"]=pd.to_numeric(eng["sql_oi_3m_pct"],errors="coerce")
    eng["session_px"]=pd.to_numeric(eng.get("spot_change_pct_t0"),errors="coerce")
    eng["cumoi"]=pd.to_numeric(eng.future_oi_change_pct_t0,errors="coerce")
    eng["pcr"]=pd.to_numeric(eng.pcr,errors="coerce")
    eng["pcrd"]=pd.to_numeric(eng["sql_pcr_change_3m"],errors="coerce")

    # Sep-11 old index collector did not store the new traded-value 3m money flow.
    # Do not mislabel fresh-OI value as money flow; mark this component unavailable.
    if "total_flow_3m_cr" in eng.columns and pd.to_numeric(eng["total_flow_3m_cr"],errors="coerce").notna().any():
        eng["flow"]=pd.to_numeric(eng["total_flow_3m_cr"],errors="coerce")
        MONEY_FLOW_AVAILABLE=True
    else:
        eng["flow"]=float("nan")
        MONEY_FLOW_AVAILABLE=False

    if eng.empty:
        raise RuntimeError("All engine rows were lost during timestamp normalization")

    print("SCORING EXACT-SQL NIFTY RECONSTRUCTION...")
    print("OI threshold: +0.05%; second OI point = 3 consecutive >= +0.05%")
    print("Qty imbalance threshold: +/-25%")
    print(f"Historical 3m money flow available: {MONEY_FLOW_AVAILABLE}")
    print("NOTE: when unavailable, score is observed score / 8.5 available points, not a true zero for money flow.")
    rows=[]
    for i,r in eng.iterrows():
        hist=eng.iloc[:i+1]
        pxs=hist.px3.dropna().tail(3)
        up=int((pxs>0.05).sum()); dn=int((pxs<-0.05).sum())
        bp=2 if len(pxs)>=3 and up==3 else 1 if up>=2 else 0
        sp=2 if len(pxs)>=3 and dn==3 else 1 if dn>=2 else 0

        sign=1 if r.session_px>0 else -1 if r.session_px<0 else 0
        boi=soi=0
        # NIFTY calibration: +0.05% is approximately an upper-quartile
        # positive 3-minute OI event on 11-Sep.
        if pd.notna(r.oi3) and r.oi3>=0.05:
            boi+=1 if sign>0 else 0
            soi+=1 if sign<0 else 0

        # Second OI point rewards persistent fresh positioning instead of
        # requiring cumulative OI to exceed +1%.
        recent_oi=hist.oi3.dropna().tail(3)
        persistent_fresh_oi=(len(recent_oi)>=3 and (recent_oi>=0.05).all())
        if persistent_fresh_oi:
            boi+=1 if sign>0 else 0
            soi+=1 if sign<0 else 0

        states=[]
        for _,z in hist.iterrows():
            if z.px3>0 and z.oi3>0: states.append("LONG_BUILDUP")
            elif z.px3<0 and z.oi3>0: states.append("SHORT_BUILDUP")
            elif z.px3>0 and z.oi3<0: states.append("SHORT_COVERING")
            elif z.px3<0 and z.oi3<0: states.append("LONG_UNWINDING")
            else: states.append("MIXED")
        fs=states[-1]; streak=1
        for z in reversed(states[:-1]):
            if z==fs: streak+=1
            else: break
        stpts=2 if streak>=3 else 1 if streak>=2 else 0
        bst=stpts if fs=="LONG_BUILDUP" else 0
        sst=stpts if fs=="SHORT_BUILDUP" else 0

        prior=hist.flow.iloc[:-1].dropna().tail(5)
        flowx=(r.flow/prior.mean()) if len(prior)>=3 and prior.mean()>0 else None
        flpts=1.5 if flowx is not None and flowx>=2 else 1 if flowx is not None and flowx>=1.5 else 0
        bfl=flpts if r.px3>0 else 0; sfl=flpts if r.px3<0 else 0

        pc=hist.pcrd.dropna().tail(3); ptr=float(pc.sum()) if len(pc)>=2 else None
        bpcr=1 if ptr is not None and ptr>0 and int((pc>0).sum())>=2 else 0
        spcr=1 if ptr is not None and ptr<0 and int((pc<0).sum())>=2 else 0

        td=imb=None
        if not agg.empty:
            a=agg[agg.ts<=r.ts]
            if not a.empty:
                a=a.iloc[-1]
                td=pd.to_numeric(a.get("delta_pct"),errors="coerce")
                imb=pd.to_numeric(a.get("total_qty_imbalance"),errors="coerce")
        bag=1 if pd.notna(td) and td>=30 else 0; sag=1 if pd.notna(td) and td<=-30 else 0
        bimb=.5 if pd.notna(imb) and imb>=25 else 0
        simb=.5 if pd.notna(imb) and imb<=-25 else 0

        bull=bp+boi+bst+bfl+bpcr+bag+bimb
        bear=sp+soi+sst+sfl+spcr+sag+simb
        score=max(bull,bear); direction="LONG" if bull>bear else "SHORT" if bear>bull else "MIXED"
        state=("HIGH CONVICTION " if score>=8.5 else "CONFIRMED " if score>=7 else "BUILDING " if score>=6 else "WATCH " if score>=4 else "NEUTRAL ")
        rows.append((DAY,SYMBOL,r.ts,max(bp,sp),max(boi,soi),fs,streak,stpts,flowx,flpts,ptr,max(bpcr,spcr),max(bag,sag),max(bimb,simb),bull,bear,score,direction,state+direction,r.spot,r.session_px,r.px3,r.oi3,r.cumoi,r.pcr,td,imb))

    sql=f"""INSERT INTO public.{TARGET} VALUES ({','.join(['%s']*27)})
    ON CONFLICT(trading_date,symbol,ts) DO UPDATE SET
    score=EXCLUDED.score,direction=EXCLUDED.direction,state=EXCLUDED.state,
    bull_score=EXCLUDED.bull_score,bear_score=EXCLUDED.bear_score"""
    with db() as c:
        with c.cursor() as x:
            x.executemany(sql,rows)
            x.execute(f"""SELECT ts,score,direction,state FROM public.{TARGET}
              WHERE trading_date=%s AND symbol=%s ORDER BY score DESC,ts LIMIT 1""",(DAY,SYMBOL))
            peak=x.fetchone()
        c.commit()

    print(f"COMPLETE rows={len(rows)}")
    if not rows:
        print("NO SCORED ROWS: Sep-11 source rows were found, but none survived timestamp/scoring preparation.")
        return

    # Use the in-memory scored rows as the authoritative backtest result.
    # This avoids a misleading crash if a post-insert SELECT returns no row.
    peak_row = sorted(rows, key=lambda r: (-float(r[16]), r[2]))[0]
    peak_ts = peak_row[2]
    if getattr(peak_ts, "tzinfo", None) is None:
        peak_ts = peak_ts.tz_localize("UTC") if hasattr(peak_ts, "tz_localize") else peak_ts.replace(tzinfo=ZoneInfo("UTC"))
    peak_local = peak_ts.tz_convert(IST) if hasattr(peak_ts, "tz_convert") else peak_ts.astimezone(IST)

    print(
        f"PEAK score={peak_row[16]} direction={peak_row[17]} "
        f"time={peak_local:%H:%M} IST state={peak_row[18]}"
    )
    print(
        "PEAK COMPONENTS | "
        f"price={peak_row[3]}/2 | oi={peak_row[4]}/2 | state={peak_row[7]}/2 | "
        f"flow={'N/A' if not MONEY_FLOW_AVAILABLE else str(peak_row[9])+'/1.5'} | "
        f"pcr={peak_row[11]}/1 | aggression={peak_row[12]}/1 | imbalance={peak_row[13]}/0.5"
    )
    available_max = 10.0 if MONEY_FLOW_AVAILABLE else 8.5
    print(f"PEAK OBSERVED SCORE = {peak_row[16]} / {available_max} available points")

    for threshold in (4,6,7,8,8.5):
        hit=next((r for r in rows if float(r[16])>=threshold),None)
        if hit:
            hts=hit[2]
            if getattr(hts, "tzinfo", None) is None:
                hts=hts.tz_localize("UTC") if hasattr(hts, "tz_localize") else hts.replace(tzinfo=ZoneInfo("UTC"))
            hlocal=hts.tz_convert(IST) if hasattr(hts, "tz_convert") else hts.astimezone(IST)
            print(f"FIRST >= {threshold}: {hlocal:%H:%M} IST | {hit[16]} | {hit[17]} | {hit[18]}")
        else:
            print(f"FIRST >= {threshold}: NONE")

if __name__=="__main__": main()
