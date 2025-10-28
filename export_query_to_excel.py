import csv
import sys
from datetime import datetime
import os
import mysql.connector

DB_HOST = "tocalivrosdb.c3uprzzalu4x.us-east-1.rds.amazonaws.com"
DB_USER = "yuri"
DB_PASSWORD = "Y!U@R#I$:)." 
DB_NAME = "tocalivros"
DB_PORT = 3306
DB_CHARSET = "utf8mb3"  
DB_COLLATION = "utf8mb3_general_ci"

OUTPUT_DIR = r"C:\Users\yurim\Desktop\relatorios"

os.makedirs(OUTPUT_DIR, exist_ok=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
CSV_FILEPATH = os.path.join(OUTPUT_DIR, f"Audible {timestamp}.csv")

SQL_QUERY = """
SELECT 
    par.nome_fantasia AS "Distribuidora",
    p.sku,
    p.nome,
    pe.editora_nome,
    pd.formato,
    pd.status,
    pd.distribuicao_enviado_status,
    pd.distriubicao_enviado_data
FROM 
	produto_distribuicao pd
LEFT JOIN 
	produto p ON p.sku = pd.sku
LEFT JOIN 
	produto_editora pe ON pe.id_editora = p.id_editora
LEFT JOIN 
	parceiros par ON par.id_parceiros = pd.id_parceiros
WHERE 
    pd.status = 'ativo'
    AND pd.formato = 'A'
    AND pd.id_parceiros = 1196
"""

try:
    print(f"Conectando ao banco '{DB_NAME}' em {DB_HOST}…")
    connection = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset=DB_CHARSET,
        collation=DB_COLLATION,
        use_pure=True
    )

    if connection.is_connected():
        print("Conexão estabelecida com sucesso!")

    cursor = connection.cursor(dictionary=True)

    print("Executando a query…")
    cursor.execute(SQL_QUERY)
    results = cursor.fetchall()

    if not results:
        print("⚠️ Nenhum resultado encontrado.")
        sys.exit(0)

    print(f"✅ Query executada: {len(results)} linhas retornadas.")

    headers = list(results[0].keys())

    for row in results:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.strftime("%Y-%m-%d %H:%M:%S")

    with open(CSV_FILEPATH, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter=";")
        writer.writeheader()
        writer.writerows(results)

    print(f"🎉 Relatório salvo em:\n{CSV_FILEPATH}")

except mysql.connector.Error as e:
    print(f"❌ Erro MySQL: {e}")
except Exception as e:
    print(f"❌ Erro inesperado: {e}")
finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Conexão encerrada.")
