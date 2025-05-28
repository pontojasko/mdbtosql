from fastapi import FastAPI, UploadFile, File
import pypyodbc
import sqlite3
import tempfile
import os
from fastapi.responses import FileResponse

app = FastAPI()

def mdb_to_sqlite(mdb_path, sqlite_path):
    # abre conexão com o arquivo mdb
    conn = pypyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb)};DBQ=' + mdb_path + ';')
    cursor = conn.cursor()

    # cria banco sqlite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_cursor = sqlite_conn.cursor()

    # lista tabelas no mdb
    cursor.tables()
    tables = [row.table_name for row in cursor.tables() if row.table_type == "TABLE"]

    for table in tables:
        # pega esquema e dados da tabela do mdb
        cursor.execute(f"SELECT * FROM {table}")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        # cria tabela no sqlite
        cols_with_type = ", ".join([f"{col} TEXT" for col in columns])
        sqlite_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols_with_type})")

        # insere dados no sqlite
        placeholders = ", ".join(["?"] * len(columns))
        sqlite_cursor.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    sqlite_conn.commit()
    cursor.close()
    conn.close()
    sqlite_conn.close()

@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    # salva arquivo mdb temporariamente
    temp_mdb = tempfile.NamedTemporaryFile(delete=False, suffix=".mdb")
    content = await file.read()
    temp_mdb.write(content)
    temp_mdb.close()

    temp_sqlite = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
    temp_sqlite.close()

    # converte mdb para sqlite
    try:
        mdb_to_sqlite(temp_mdb.name, temp_sqlite.name)
    except Exception as e:
        return {"error": str(e)}

    # devolve arquivo sqlite para download
    return FileResponse(temp_sqlite.name, filename="converted.sqlite")

