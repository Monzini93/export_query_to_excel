import csv
import sys
from datetime import datetime
import os
import pymysql  

DB_HOST = "tocalivrosdb.c3uprzzalu4x.us-east-1.rds.amazonaws.com"
DB_USER = "yuri"
DB_PASSWORD = "Y!U@R#I$:)."
DB_NAME = "tocalivros"
DB_PORT = 3306
DB_CHARSET = "utf8mb3"

OUTPUT_DIR = r"C:\Users\yurim\Desktop\relatorios"

os.makedirs(OUTPUT_DIR, exist_ok=True)

timestamp = datetime.now().strftime("%Y-%m-%d")
CSV_FILEPATH = os.path.join(OUTPUT_DIR, f"Livos até 14 anos {timestamp}.csv")

SQL_QUERY = """
SELECT
    p.sku,
    p.nome,
    p.autor,
    p.`status`,
    pes.editora_selo AS editora,
    p.faixa_etaria,
    cate.categoria_nome AS genero,
    p.preco,
    p.formato,
    p.produtora,
    p.data_lancamento
FROM produto p
LEFT JOIN produto_editora pe
  ON pe.id_editora = p.id_editora
LEFT JOIN produto_editora_selo pes ON pes.id_editora_selo = p.id_editora_selo
LEFT JOIN 
	assinatura_tipo_produto atp ON atp.sku = p.sku
LEFT JOIN (
    SELECT
        cp.sku,
        GROUP_CONCAT(c.categoria_nome ORDER BY c.categoria_nome SEPARATOR ', ') AS categoria_nome
    FROM 
	 	categoria_produto cp
    INNER JOIN 
	 	categoria c ON c.id_categoria = cp.id_categoria
    WHERE
        c.ativo = '1'
        AND (c.path LIKE '7%' OR c.path LIKE '1%')
        AND c.nivel = 2
    GROUP BY cp.sku
) AS cate
  ON cate.sku = p.sku
WHERE
   p.faixa_etaria <= 14
	AND atp.id_assinatura_tipo = 3
	AND p.`status` = 'ativo'
"""

try:
    print(f"Conectando ao banco '{DB_NAME}' em {DB_HOST}…")
    connection = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset=DB_CHARSET,
        cursorclass=pymysql.cursors.DictCursor
    )

    print("Conexão estabelecida com sucesso!")

    with connection.cursor() as cursor:
        print("Executando a query…")
        cursor.execute(SQL_QUERY)
        results = cursor.fetchall()

        if not results:
            print("Nenhum resultado encontrado.")
            sys.exit(0)

        print(f"Query executada: {len(results)} linhas retornadas.")

        headers = list(results[0].keys())

        for row in results:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.strftime("%Y-%m-%d")

        with open(CSV_FILEPATH, "w", newline="", encoding="utf-8-sig") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter=";")
            writer.writeheader()
            writer.writerows(results)

        print(f"Relatório salvo em:\n{CSV_FILEPATH}")

except pymysql.MySQLError as e:
    print(f"Erro MySQL: {e}")
except Exception as e:
    print(f"Erro inesperado: {e}")
finally:
    if 'connection' in locals() and connection.open:
        connection.close()
        print("Conexão encerrada.")
