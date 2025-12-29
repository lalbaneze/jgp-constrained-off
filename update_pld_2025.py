import io
import sqlite3
import requests
import pandas as pd

DB_PATH = "data/pld_ccee.sqlite"
CKAN_BASE = "https://dadosabertos.ccee.org.br"
DATASET = "pld_horario"
RESOURCE_NAME = "pld_horario_2025"

def get_resource_url():
    url = f"{CKAN_BASE}/api/3/action/package_show"
    r = requests.get(url, params={"id": DATASET}, timeout=60)
    r.raise_for_status()
    pkg = r.json()["result"]

    for res in pkg["resources"]:
        name = (res.get("name") or "").strip().lower()
        if name == RESOURCE_NAME.lower():
            return res["url"]
    raise RuntimeError(f"Resource '{RESOURCE_NAME}' não encontrado no dataset '{DATASET}'.")

def ensure_tables(con: sqlite3.Connection):
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

def load_csv_to_sqlite(csv_url: str):
    print("Baixando CSV:", csv_url)
    resp = requests.get(csv_url, timeout=120)
    resp.raise_for_status()

    sample = resp.text[:2000]
    sep = ";" if sample.count(";") > sample.count(",") else ","

    df = pd.read_csv(io.StringIO(resp.text), sep=sep)
    df.columns = [c.strip().upper() for c in df.columns]

    def pick(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    col_mes = pick("MES_REFERENCIA", "MES", "MESREF")
    col_dia = pick("DIA")
    col_hora = pick("HORA", "HR")
    col_sub = pick("SUBMERCADO", "SBM")
    col_pld = pick("PLD_HORA", "PLD", "VALOR", "PRECO")

    if not all([col_mes, col_dia, col_hora, col_sub, col_pld]):
        raise RuntimeError(
            f"Colunas não reconhecidas. Achei: MES={col_mes}, DIA={col_dia}, HORA={col_hora}, SUB={col_sub}, PLD={col_pld}. "
            f"Colunas no arquivo: {df.columns.tolist()}"
        )

    df2 = df[[col_mes, col_dia, col_hora, col_sub, col_pld]].copy()
    df2.columns = ["MES_REF", "DIA_NUM", "HORA", "SUBMERCADO", "PLD_HORA"]

    # ---- CORREÇÃO: montar YYYY-MM-DD usando MES_REF (yyyymm) + DIA_NUM (dd)
    df2["MES_REF"] = pd.to_numeric(df2["MES_REF"], errors="coerce")
    df2["DIA_NUM"] = pd.to_numeric(df2["DIA_NUM"], errors="coerce")
    df2["HORA"] = pd.to_numeric(df2["HORA"], errors="coerce")

    df2 = df2.dropna(subset=["MES_REF", "DIA_NUM", "HORA", "SUBMERCADO", "PLD_HORA"])

    df2["MES_REF"] = df2["MES_REF"].astype(int)
    df2["DIA_NUM"] = df2["DIA_NUM"].astype(int)
    df2["HORA"] = df2["HORA"].astype(int)

    yyyy = (df2["MES_REF"] // 100).astype(int)
    mm = (df2["MES_REF"] % 100).astype(int)
    dd = df2["DIA_NUM"].astype(int)

    dt = pd.to_datetime(
        {"year": yyyy, "month": mm, "day": dd},
        errors="coerce"
    )
    df2["DIA"] = dt.dt.strftime("%Y-%m-%d")

    # limpeza restante (igual antes)
    df2["SUBMERCADO"] = df2["SUBMERCADO"].astype(str).str.strip().str.lower()
    df2["PLD_HORA"] = pd.to_numeric(df2["PLD_HORA"], errors="coerce")

    df2 = df2.dropna(subset=["DIA", "HORA", "SUBMERCADO", "PLD_HORA"])

    # fica só com as colunas finais da tabela
    df2 = df2[["DIA", "HORA", "SUBMERCADO", "PLD_HORA"]]

    print("Linhas no CSV (após limpeza):", len(df2))

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
    print("pld_horario linhas:", n_h)
    print("pld_medio linhas:", n_m)
    print("min dia:", min_dia, "max dia:", max_dia)

def main():
    csv_url = get_resource_url()
    load_csv_to_sqlite(csv_url)

if __name__ == "__main__":
    main()

