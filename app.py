import os
import re
from datetime import datetime
from io import StringIO

import pandas as pd
from flask import Flask, render_template, request, Response, flash, redirect, url_for
from sqlalchemy import create_engine, text, bindparam
from dotenv import load_dotenv

load_dotenv()

# ===== CONFIG BANCO =====
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "usuario")
DB_PASS = os.getenv("DB_PASS", "senha")
DB_NAME = os.getenv("DB_NAME", "seu_banco")
DB_CHARSET = os.getenv("DB_CHARSET", "utf8mb3")  # use utf8mb4 se seu RDS suportar

ENGINE = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset={DB_CHARSET}",
    pool_pre_ping=True,
)

# ===== APP =====
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "change-me")

# ===== SQL BASE (sem WHERE dinâmico) =====
SQL_BASE = """
SELECT
    p.sku,
    p.nome,
    p.autor,
    par.nome_fantasia AS editora,
    cate.categoria_nome AS genero,
    p.preco,
    p.formato,
    p.produtora,
    p.data_lancamento
FROM produto p
LEFT JOIN (
    SELECT 
        cp.sku, 
        GROUP_CONCAT(categoria_nome) AS categoria_nome 
    FROM categoria c
    INNER JOIN categoria_produto cp 
        ON cp.id_categoria = c.id_categoria
    WHERE 
        c.ativo = '1'
        AND (c.path LIKE '7%' OR c.path LIKE '1%')
        AND c.nivel = 2
    GROUP BY cp.sku
) cate 
    ON cate.sku = p.sku
LEFT JOIN produto_editora pe 
    ON pe.id_editora = p.id_editora
LEFT JOIN parceiros par 
    ON par.id_parceiros = pe.id_parceiros
-- WHERE DINÂMICO ENTRA AQUI
/**WHERE_CLAUSE**/
ORDER BY 
    par.nome_fantasia ASC,
    p.nome ASC
LIMIT :limit_rows
"""

ID_PATTERN = re.compile(r"^\s*\d+\s*(,\s*\d+\s*)*$")  # só dígitos e vírgulas

def parse_ids(raw: str) -> list[int]:
    """
    Recebe uma string como '25, 1582, 7' e devolve [25, 1582, 7].
    Lança ValueError se tiver caracteres inválidos.
    """
    raw = (raw or "").strip()
    if not raw:
        return []
    if not ID_PATTERN.match(raw):
        raise ValueError("Informe apenas números separados por vírgula (ex.: 25, 1582).")
    return [int(x.strip()) for x in raw.split(",") if x.strip()]

@app.get("/")
def index():
    return render_template("index.html")

@app.post("/export")
def export():
    try:
        ids_raw = request.form.get("ids", "")
        limit_rows = int(request.form.get("limit_rows", "20000"))  # proteção básica
        ids = parse_ids(ids_raw)
        if not ids:
            flash("Informe pelo menos um id_editora.", "warning")
            return redirect(url_for("index"))

        # Monta WHERE seguro com SQLAlchemy (expanding)
        where_txt = "WHERE p.id_editora IN :ids"
        sql = SQL_BASE.replace("/**WHERE_CLAUSE**/", where_txt)
        stmt = text(sql).bindparams(bindparam("ids", expanding=True))
        params = {"ids": ids, "limit_rows": limit_rows}

        with ENGINE.connect() as conn:
            df = pd.read_sql(stmt, conn, params=params)

        # Gera CSV em memória com BOM (Excel-friendly) e ; como separador
        csv_buf = StringIO()
        df.to_csv(csv_buf, index=False, sep=";")
        csv_bytes = ("\ufeff" + csv_buf.getvalue()).encode("utf-8")  # UTF-8 BOM

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"relatorio_produtos_{ts}.csv"
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "text/csv; charset=utf-8",
        }
        return Response(csv_bytes, headers=headers)

    except ValueError as ve:
        flash(str(ve), "danger")
        return redirect(url_for("index"))
    except Exception as e:
        # Em produção, logue o erro (Sentry/CloudWatch). Aqui mostramos feedback simples.
        flash(f"Erro ao gerar relatório: {e}", "danger")
        return redirect(url_for("index"))

if __name__ == "__main__":
    # flask run também funciona. Aqui permite rodar com: python app.py
    app.run(host="0.0.0.0", port=8000, debug=True)
    print(f"Erro ao gerar relatório: {e}")