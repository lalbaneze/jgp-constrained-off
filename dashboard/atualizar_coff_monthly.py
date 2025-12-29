import os
import json
from datetime import datetime

import pandas as pd
import requests

ONS_BASE = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/restricao_coff_eolica_tm"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

OUT_CSV = os.path.join(DATA_DIR, "coff_eolica_monthly.csv")
MAP_PATH = os.path.join(DATA_DIR, "mapping_citi.json")


def load_mapping():
    if not os.path.exists(MAP_PATH):
        return {}
    with open(MAP_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_month(yyyy_mm: str) -> pd.DataFrame:
    url = f"{ONS_BASE}/RESTRICAO_COFF_EOLICA_{yyyy_mm}.csv"
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        return pd.DataFrame()
    from io import StringIO
    return pd.read_csv(StringIO(r.text), sep=";", encoding="utf-8")


def last_ym_existing() -> str | None:
    if not os.path.exists(OUT_CSV):
        return None
    try:
        old = pd.read_csv(OUT_CSV)
        if old.empty or "ym" not in old.columns:
            return None
        v = old["ym"].dropna()
        if v.empty:
            return None
        return str(v.max())
    except Exception:
        return None


def ym_to_dt(ym: str) -> datetime:
    y, m = ym.split("-")
    return datetime(int(y), int(m), 1)


def dt_to_yyyy_mm(dt: datetime) -> str:
    return f"{dt.year}_{dt.month:02d}"


def months_from(start_ym: str) -> list[str]:
    start = ym_to_dt(start_ym)
    cur = datetime(start.year, start.month, 1)
    end = datetime.today().replace(day=1)
    out = []
    while cur <= end:
        out.append(dt_to_yyyy_mm(cur))
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1)
        else:
            cur = datetime(cur.year, cur.month + 1, 1)
    return out


def map_empresa(nom_usina: str, mapping: dict) -> str:
    if not isinstance(mapping, dict):
        return nom_usina
    if "usina_to_empresa" in mapping and isinstance(mapping["usina_to_empresa"], dict):
        return mapping["usina_to_empresa"].get(nom_usina, nom_usina)
    return mapping.get(nom_usina, nom_usina)


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    mapping = load_mapping()
    last = last_ym_existing()

    if last:
        start_ym = last
        print(f"→ CSV existente. Último ym: {last}. Vou atualizar de {start_ym} em diante.")
    else:
        start_ym = "2025-01"
        print("→ CSV não encontrado/sem ym. Fallback: 2025-01")

    months = months_from(start_ym)

    dfs = []
    for m in months:
        print(f"   - baixando {m} ...")
        dfm = fetch_month(m)
        if not dfm.empty:
            dfs.append(dfm)

    if not dfs:
        print("❌ Nenhum dado baixado do ONS.")
        return

    df = pd.concat(dfs, ignore_index=True)

    # valida colunas ONS
    needed = {"din_instante", "nom_usina", "val_geracao", "val_geracaoreferenciafinal"}
    if not needed.issubset(df.columns):
        raise SystemExit(
            f"Colunas ONS faltando. Esperadas: {sorted(list(needed))}\n"
            f"Encontradas: {list(df.columns)}"
        )

    df["din_instante"] = pd.to_datetime(df["din_instante"], errors="coerce")
    df = df.dropna(subset=["din_instante"])

    df["ger_mwmed"] = pd.to_numeric(df["val_geracao"], errors="coerce")
    df["ref_mwmed"] = pd.to_numeric(df["val_geracaoreferenciafinal"], errors="coerce")
    df = df.dropna(subset=["ger_mwmed", "ref_mwmed"])

    df["coff_mwmed"] = (df["ref_mwmed"] - df["ger_mwmed"]).clip(lower=0.0)

    # assumindo base horária (MWmed por hora -> MWh)
    df["ger_mwh"] = df["ger_mwmed"]
    df["coff_mwh"] = df["coff_mwmed"]

    df["empresa"] = df["nom_usina"].astype(str).map(lambda x: map_empresa(x, mapping))
    df["empresa"] = df["empresa"].fillna("N/A").astype(str).str.strip()
    df["ym"] = df["din_instante"].dt.strftime("%Y-%m").astype(str).str.strip()

    df = df[(df["empresa"] != "") & (df["ym"] != "")]

    novo = (
        df.groupby(["ym", "empresa"], dropna=False, as_index=False)[["coff_mwh", "ger_mwh"]]
          .sum()
          .sort_values(["ym", "empresa"])
    )

    # coff_pct sem apply (evita indentação multilinha e é mais rápido)
    novo["coff_pct"] = novo["coff_mwh"] / novo["ger_mwh"]
    novo.loc[novo["ger_mwh"] <= 0, "coff_pct"] = pd.NA

    # merge com histórico: remove meses recalculados e concatena
    if os.path.exists(OUT_CSV):
        old = pd.read_csv(OUT_CSV)
        if not old.empty and "ym" in old.columns:
            old = old[~old["ym"].isin(novo["ym"].unique())]
            final = pd.concat([old, novo], ignore_index=True)
        else:
            final = novo
    else:
        final = novo

    final = final.sort_values(["ym", "empresa"])
    final.to_csv(OUT_CSV, index=False, float_format="%.6f")

    print("✅ Atualizado com sucesso:")
    print(f"   {OUT_CSV}")
    print(f"   meses atualizados: {sorted(novo['ym'].unique())}")


if __name__ == "__main__":
    main()
