# pld_ccee/src/update_pld_2025.py
import io
import os
import sqlite3
import requests
import pandas as pd

CKAN_BASE = "https://dadosabertos.ccee.org.br"
DATASET = "pld_horario"
RESOURCE_NAME = "pld_horario_2025"

# Base = .../pld_ccee
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "pld_ccee.sqlite")


def get_resource_url() -> str:
    url = f"{CKAN_BASE}/api/3/action/package_show"
    r = requests.get(url, params={"id": DATASET}, timeout=60)
    r.raise_for_status()
    pkg = r.json()["result"]

    for res in pkg["resources"]:
        name = (res.get("name") or "").strip().lower()
        if name == RESOURCE_NAME.lower():
            return res["url"]

    raise RuntimeError(
        f"Resource '{RESOURCE_NAME}' não encontrado no dataset '{DATASET}'."
    )


def ensure_tables(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pld_horario (
            DIA TEXT,
            HORA INTEGER,
            SUBMERCADO TEXT,
            PLD_HORA REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pld_medio (
            DIA TEXT,
            HORA INTEGER,
            PLD_MEDIO REAL
        )
    """)
    con.commit()


def load_csv_to_sqlite(csv_url: str) -> None:
    print("Baixando CSV:", csv_url)
    resp = requests.get(csv_url, timeout=120)
    resp.raise_for_status()

    # Detecta separador ; ou ,
    sample = resp.text[:2000]
    sep = ";" if sample.count(";") > sample.count(",") else ","

    df = pd.read_csv(io.StringIO(resp.text), sep=sep)
    df.columns = [c.strip().upper() for c in df.columns]

    print("Colunas do CSV:", df.columns.tolist())
    print(df.head(3).to_string(index=False))

    def pick(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    col_dia = pick("DIA", "DATA", "DT_DIA", "DT_REFERENCIA")
    col_hora = pick("HORA", "HR", "HORA_INICIO")
    col_sub = pick("SUBMERCADO", "SUBMERCADO_SBM", "SBM")
    col_pld = pick("PLD_HORA", "PLD", "VALOR", "PRECO")

    if not all([col_dia, col_hora, col_sub, col_pld]):
        raise RuntimeError(
            f"Colunas não reconhecidas. Achei: DIA={col_dia}, HORA={col_hora}, "
            f"SUB={col_sub}, PLD={col_pld}. Colunas no arquivo: {df.columns.tolist()}"
        )

    df2 = df[[col_dia, col_hora, col_sub, col_pld]].copy()
    df2.columns = ["DIA", "HORA", "SUBMERCADO", "PLD_HORA"]

    # ---- Limpeza / Normalização do DIA ----
    d = df2["DIA"].astype(str).str.strip()

    # Caso venha YYYY-MM-DD
    dt = pd.to_datetime(d, errors="coerce", format="%Y-%m-%d")

    # Se falhar, tenta DD/MM/YYYY
    mask = dt.isna()
    if mask.any():
        dt2 = pd.to_datetime(d[mask], errors="coerce", dayfirst=True)
        dt.loc[mask] = dt2

    df2["DIA"] = dt.dt.strftime("%Y-%m-%d")

    df2["HORA"] = pd.to_numeric(df2["HORA"], errors="coerce").fillna(0).astype(int)
    df2["SUBMERCADO"] = df2["SUBMERCADO"].astype(str).str.strip().str.lower()
    df2["PLD_HORA"] = pd.to_numeric(df2["PLD_HORA"], errors="coerce")

    df2 = df2.dropna(subset=["DIA", "HORA", "SUBMERCADO", "PLD_HORA"])
    df2 = df2[df2["DIA"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)]

    print("Linhas no CSV (após limpeza):", len(df2))

    # Garante pasta do DB
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    con = sqlite3.connect(DB_PATH)
    ensure_tables(con)
    cur = con.cursor()

    print("Limpando tabela pld_horario...")
    cur.execute("DELETE FROM pld_horario")

    print("Inserindo em pld_horario...")
    df2.to_sql("pld_horario", con, if_exists="append", index=False)

    print("Recalculando pld_medio...")
    cur.execute("DELETE FROM pld_medio")
    cur.execute("""
        INSERT INTO pld_medio (DIA, HORA, PLD_MEDIO)
        SELECT DIA, HORA, AVG(PLD_HORA)
        FROM pld_horario
        GROUP BY DIA, HORA
    """)

    con.commit()

    n_h = cur.execute("SELECT COUNT(*) FROM pld_horario").fetchone()[0]
    n_m = cur.execute("SELECT COUNT(*) FROM pld_medio").fetchone()[0]
    max_dia = cur.execute("SELECT MAX(DIA) FROM pld_medio").fetchone()[0]
    min_dia = cur.execute("SELECT MIN(DIA) FROM pld_medio").fetchone()[0]

    con.close()

    print("OK ✅")
    print("DB:", DB_PATH)
    print("pld_horario linhas:", n_h)
    print("pld_medio linhas:", n_m)
    print("min dia:", min_dia, "max dia:", max_dia)


def main():
    csv_url = get_resource_url()
    load_csv_to_sqlite(csv_url)


if __name__ == "__main__":
    main()

