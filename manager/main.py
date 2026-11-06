import time

from celery import group
from workers.celery_app import app


def main():

    print("=" * 55)
    print("IBGE DATA PROCESSING SYSTEM")
    print("MANAGER - COORDENADOR CENTRAL")
    print("=" * 55)

    # Tabelas que serão processadas
    tables = [
        8880,  # Comércio varejista
        8888   # Produção física industrial
    ]

    print("\n[MANAGER] Distribuindo tarefas...")

    tasks = group(
        app.signature(
            "workers.tasks.fetch_table",
            args=[table]
        )
        for table in tables
    )

    job = tasks.apply_async()

    print("[MANAGER] Aguardando Workers...")

    results = job.get(
        timeout=600,
        propagate=True
    )

    for result in results:
        print("[MANAGER] Resultado:", result)

    print("\n[MANAGER] Solicitando análise integrada...")

    analysis = app.send_task(
        "workers.tasks.analyze_tables"
    )

    report = analysis.get(
        timeout=600,
        propagate=True
    )

    print("\n[MANAGER] RELATÓRIO FINAL")
    print(report)

    print("\n[MANAGER] PROCESSAMENTO CONCLUÍDO")


if __name__ == "__main__":
    main()
