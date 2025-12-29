# export_pld_json.py
import json
import os
import sqlite3
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ajuste aqui se o export_pld_json.py estiver em coff_eolica_2025/
DB_PATH = os.path.abspath(
    os.path.join(
        BASE_DIR,
        "..",              # sobe para "Lisa - JGP Gestão de Recursos"
        "pld_ccee",
        "data",
        "pld_ccee.sqlite"
    )
)

OUT_DIR = os.path.join(BASE_DIR, "dashboard", "data")
OUT_MONTHLY = os.path.join(OUT_DIR, "pld_monthly_avg.json")
OUT_META = os.path.join(OUT_DIR, "pld_meta.json")


def table_cols(con, table):
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    # row: (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]


def pick_date_expr(cols):
    """
    Retorna um SQL expression que vira 'YYYY-MM-DD' + um WHERE opcional.
    """
    # caso 1: já existe uma coluna DIA como YYYY-MM-DD
    if "DIA" in cols:
        # checa se parece data (length 10)
        return "DIA", "WHERE length(DIA)=10"

    # caso 2: colunas separadas (variações comuns)
    # (ajuste aqui se seu PRAGMA mostrar outro nome)
    cand_sets = [
        ("ANO", "MES", "DIA"),
        ("ano", "mes", "dia"),
        ("Ano", "Mes", "Dia"),
    ]
    for a, m, d in cand_sets:
        if a in cols and m in cols and d in cols:
            # monta YYYY-MM-DD
            return f"printf('%04d-%02d-%02d', {a}, {m}, {d})", ""

    # caso 3: alguma coluna DATA/DATE
    for c in ["DATA", "data", "Date", "DATE"]:
        if c in cols:
            return c, ""

    return None, None


def pick_pld_col(cols):
    # tenta achar o nome do PLD médio
    for c in ["PLD_MEDIO", "pld_medio", "VALOR", "valor", "PLD", "pld"]:
        if c in cols:
            return c
    return None


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB não encontrado: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)

    cols = table_cols(con, "pld_medio")
    date_expr, date_where = pick_date_expr(cols)
    pld_col = pick_pld_col(cols)

    if not date_expr or not pld_col:
        con.close()
        raise SystemExit(
            "Não consegui identificar colunas do pld_medio.\n"
            f"Colunas encontradas: {cols}\n"
            "Rode PRAGMA table_info(pld_medio) e me mande o print."
        )

    # média mensal por YYYY-MM
    sql_monthly = f"""
      SELECT substr({date_expr},1,7) as ym, AVG({pld_col}) as pld_medio_mensal
      FROM pld_medio
      {date_where}
      GROUP BY substr({date_expr},1,7)
      ORDER BY ym
    """
    rows = con.execute(sql_monthly).fetchall()

    monthly = {}
    for ym, val in rows:
        monthly[str(ym)] = float(val) if val is not None else None

    # max dia disponível
    sql_max = f"""
      SELECT MAX({date_expr}) as max_dia
      FROM pld_medio
      {date_where}
    """
    rmax = con.execute(sql_max).fetchone()
    max_dia = rmax[0] if rmax else None

    con.close()

    with open(OUT_MONTHLY, "w", encoding="utf-8") as f:
        json.dump(monthly, f, ensure_ascii=False, indent=2)

    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump({
            "max_dia": max_dia,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }, f, ensure_ascii=False, indent=2)

    print("✅ Gerados:")
    print(" -", OUT_MONTHLY)
    print(" -", OUT_META)
    print("PLD max_dia:", max_dia)
    print("Meses no JSON:", len(monthly))


if __name__ == "__main__":
    main()

