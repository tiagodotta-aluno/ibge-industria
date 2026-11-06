import os
import json
import re
import requests
import pandas as pd
import psycopg2

from psycopg2.extras import Json
from workers.celery_app import app

API = os.getenv(
    "SIDRA_API",
    "https://apisidra.ibge.gov.br/values"
)

DATABASE_URL = os.getenv("DATABASE_URL")

RAW = "/data/raw"
PROCESSED = "/data/processed"


def save_database(table_id, records):

    if not DATABASE_URL:
        return

    conn = psycopg2.connect(DATABASE_URL)

    try:
        with conn:
            with conn.cursor() as cur:

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sidra_raw (
                        id SERIAL PRIMARY KEY,
                        table_id INTEGER,
                        payload JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                for record in records:
                    cur.execute("""
                        INSERT INTO sidra_raw
                        (table_id, payload)
                        VALUES (%s, %s)
                    """, (table_id, Json(record)))

    finally:
        conn.close()


@app.task(
    name="workers.tasks.fetch_table",
    bind=True,
    max_retries=3
)
def fetch_table(self, table_id):

    try:

        url = (
            f"{API}/t/{table_id}"
            "/n1/1/v/all/p/last%2024"
        )

        response = requests.get(
            url,
            timeout=90
        )

        response.raise_for_status()

        records = response.json()

        if not records:
            raise ValueError(
                f"Tabela {table_id} sem registros."
            )

        os.makedirs(RAW, exist_ok=True)

        path = f"{RAW}/sidra_{table_id}.json"

        with open(path, "w", encoding="utf-8") as file:
            json.dump(
                records,
                file,
                ensure_ascii=False,
                indent=2
            )

        # Persiste os dados originais
        save_database(table_id, records[1:])

        return {
            "table_id": table_id,
            "records": len(records) - 1,
            "file": path,
            "status": "success"
        }

    except Exception as exc:

        raise self.retry(
            exc=exc,
            countdown=10
        )


def normalize_records(records, table_id):

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records[1:])

    if df.empty or "V" not in df.columns:
        return pd.DataFrame()

    dimension_columns = [
        col for col in df.columns
        if re.fullmatch(r"D\d+N", col)
    ]

    code_columns = [
        col for col in df.columns
        if re.fullmatch(r"D\d+C", col)
    ]

    def extract_period(row):

        for column in code_columns:

            value = str(row.get(column, ""))

            if re.fullmatch(r"20\d{4}", value):
                return value

        return None

    df["periodo"] = df.apply(
        extract_period,
        axis=1
    )

    df["valor"] = pd.to_numeric(
        df["V"].astype(str).str.replace(",", "."),
        errors="coerce"
    )

    df["tabela"] = table_id

    df["serie"] = df.apply(
        lambda row: " | ".join(
            str(row[col])
            for col in dimension_columns
            if pd.notna(row[col])
        ),
        axis=1
    )

    return df.dropna(
        subset=["periodo", "valor"]
    )


@app.task(name="workers.tasks.analyze_tables")
def analyze_tables():

    os.makedirs(PROCESSED, exist_ok=True)

    tables = [8880, 8888]
    datasets = []

    for table_id in tables:

        path = f"{RAW}/sidra_{table_id}.json"

        with open(path, encoding="utf-8") as file:
            records = json.load(file)

        df = normalize_records(
            records,
            table_id
        )

        if not df.empty:
            datasets.append(df)

    if not datasets:
        return {"status": "no_data"}

    result = pd.concat(
        datasets,
        ignore_index=True
    )

    result.to_csv(
        f"{PROCESSED}/dados_integrados.csv",
        index=False
    )

    # Identificação das séries relevantes
    result["serie_busca"] = (
        result["serie"].str.lower()
    )

    pmc = result[
        (result["tabela"] == 8880)
        & result["serie_busca"].str.contains(
            "volume|comércio varejista|total",
            regex=True,
            na=False
        )
    ]

    industria = result[
        (result["tabela"] == 8888)
        & result["serie_busca"].str.contains(
            "indústria geral|produção física",
            regex=True,
            na=False
        )
    ]

    def build_series(df):

        if df.empty:
            return None

        return (
            df.groupby("periodo")["valor"]
            .mean()
            .sort_index()
        )

    serie_pmc = build_series(pmc)
    serie_industria = build_series(industria)

    correlation = None

    if serie_pmc is not None and serie_industria is not None:

        comparison = pd.concat(
            [
                serie_pmc.rename("comercio"),
                serie_industria.rename("industria")
            ],
            axis=1,
            join="inner"
        ).dropna()

        if len(comparison) >= 3:

            correlation = float(
                comparison["comercio"].corr(
                    comparison["industria"]
                )
            )

    report = {
        "registros": len(result),
        "periodos": sorted(
            result["periodo"].unique().tolist()
        ),
        "correlacao": correlation,
        "interpretacao": (
            "Correlação calculada."
            if correlation is not None
            else "Séries comparáveis não identificadas. "
                 "Verifique os metadados e classificações."
        )
    }

    with open(
        f"{PROCESSED}/relatorio.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            ensure_ascii=False,
            indent=2
        )

    return report
