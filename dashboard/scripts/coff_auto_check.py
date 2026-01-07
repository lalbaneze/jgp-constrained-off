import os
import sys
import pandas as pd

def monthly_tot(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]
    # compat: se vier pct_curtailment, não importa, usamos os MWh
    for col in ["mes", "curtailment_mwh", "generation_mwh"]:
        if col not in df.columns:
            raise SystemExit(f"Faltando coluna {col} em {path}")

    df["curtailment_mwh"] = pd.to_numeric(df["curtailment_mwh"], errors="coerce").fillna(0.0)
    df["generation_mwh"] = pd.to_numeric(df["generation_mwh"], errors="coerce").fillna(0.0)

    t = (df.groupby("mes", as_index=False)[["curtailment_mwh","generation_mwh"]]
           .sum()
           .sort_values("mes"))
    t["pct_total"] = t["curtailment_mwh"] / t["generation_mwh"].where(t["generation_mwh"] > 0, 1)
    return t.set_index("mes")

def main():
    # uso: python coff_auto_check.py eolica|solar
    if len(sys.argv) < 2 or sys.argv[1].lower() not in ("eolica", "solar"):
        raise SystemExit("Uso: python dashboard/scripts/coff_auto_check.py eolica|solar")

    kind = sys.argv[1].lower()

    official = os.path.join("dashboard", "data", f"coff_{kind}_monthly.csv")
    test = os.path.join("dashboard", "data", f"coff_{kind}_monthly_test.csv")

    old = monthly_tot(official)
    new = monthly_tot(test)

    common = sorted(set(old.index) & set(new.index))
    old = old.loc[common]
    new = new.loc[common]

    cmp = pd.DataFrame({
        "old_cut": old["curtailment_mwh"],
        "new_cut": new["curtailment_mwh"],
        "old_pct": old["pct_total"],
        "new_pct": new["pct_total"],
    })

    # diffs
    cmp["cut_diff_%"] = (cmp["new_cut"] / cmp["old_cut"].replace(0, pd.NA) - 1) * 100
    cmp["pct_diff_pp"] = (cmp["new_pct"] - cmp["old_pct"]) * 100

    # meses que mudaram (tolerância pequena)
    changed = cmp[
        (cmp["old_cut"].round(2) != cmp["new_cut"].round(2)) |
        (cmp["old_pct"].round(6) != cmp["new_pct"].round(6))
    ].copy()

    # se não mudou nada
    if changed.empty:
        verdict = "NO-CHANGES"
        summary = f"{kind.upper()}: nenhuma diferença por mês."
        print(f"verdict={verdict}")
        print(f"summary={summary}")
        print("changed_months=")
        return

    last_month = cmp.index.max()
    changed_months = list(changed.index)

    only_last_month = (len(changed_months) == 1 and changed_months[0] == last_month)

    # limites do “auto-ok”
    cut_diff = float(changed.loc[last_month, "cut_diff_%"]) if last_month in changed.index else None
    pct_pp = float(changed.loc[last_month, "pct_diff_pp"]) if last_month in changed.index else None

    within_2pct = (cut_diff is not None and abs(cut_diff) < 2.0)

    if only_last_month and within_2pct:
        verdict = "AUTO-OK"
    else:
        verdict = "REVIEW"

    # resumo curto pra colocar no PR
    if only_last_month:
        summary = (f"{kind.upper()}: mudou só {last_month}. "
                   f"Δ corte={cut_diff:.2f}% | Δ pct={pct_pp:.3f} p.p. → {verdict}")
    else:
        summary = (f"{kind.upper()}: mudaram {len(changed_months)} mês(es): {', '.join(changed_months)} → {verdict}")

    print(f"verdict={verdict}")
    print(f"summary={summary}")
    print(f"changed_months={','.join(changed_months)}")

if __name__ == "__main__":
    main()
