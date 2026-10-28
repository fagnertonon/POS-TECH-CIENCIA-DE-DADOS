"""Read-only audit of the exact Silver snapshot used by the committed Gold run.

Reads S3 Parquet one file/batch at a time. Does not call Glue, BigQuery or write S3.
All emitted information is aggregated; student identifiers are not read.
"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
from urllib.parse import urlsplit

GOLD_MANIFEST = (
    "s3://fiap-tc-fase2-2026/auditoria/gold/ingestion_date=2026-10-22/"
    "run_id=jr_74dca58f19d0bd28dd27bba71b3c1a0bc420e55ec2dab74fb047e23b1901d4cd/manifest.json"
)
UF = dict(zip(
    "11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 41 42 43 50 51 52 53".split(),
    "RO AC AM RR PA AP TO MA PI CE RN PB PE AL SE BA MG ES RJ SP PR SC RS MS MT GO DF".split()))
COUNTERS = ["registros", "avaliados", "avaliacao_nula", "alfabetizacao_nula_entre_avaliados",
            "peso_invalido_entre_avaliados", "elegiveis_calculo", "alfabetizados",
            "flag_avaliado_diverge_dicionario", "flag_alfabetizado_diverge_dicionario",
            "codigo_categorico_sem_mapeamento"]
LABELS = {"presenca": {"Presente": True, "Ausente": False},
          "preenchimento_caderno": {"Prova preenchida": True, "Prova não preenchida": False},
          "alfabetizado": {"Sim": True, "Não": False}}


def split_s3(uri):
    p = urlsplit(uri)
    if p.scheme != "s3" or not p.netloc or not p.path.strip("/") or p.query or p.fragment:
        raise ValueError("Expected exact s3://bucket/key")
    if any(v in (".", "..") for v in p.path.split("/")):
        raise ValueError("Invalid S3 path")
    return p.netloc, p.path.lstrip("/")


def three_and(left, right):
    if left is False or right is False:
        return False
    return True if left is True and right is True else None


def kahan_add(accumulator, value):
    y = value - accumulator[1]
    total = accumulator[0] + y
    accumulator[1] = (total - accumulator[0]) - y
    accumulator[0] = total


def new_stats():
    return {**{name: 0 for name in COUNTERS}, "peso": [0.0, 0.0], "peso_alfabetizados": [0.0, 0.0]}


def add_row(stats, row, dictionary):
    stats["registros"] += 1
    decoded = {}
    for column in LABELS:
        raw = row.get(column)
        label = dictionary.get(column, {}).get(str(raw)) if raw is not None else None
        decoded[column] = LABELS[column].get(label)
        if raw is not None and label not in LABELS[column]:
            stats["codigo_categorico_sem_mapeamento"] += 1
    expected_evaluated = three_and(decoded["presenca"], decoded["preenchimento_caderno"])
    expected_alf = decoded["alfabetizado"] if expected_evaluated is True else None
    evaluated, alf, weight = row.get("avaliado"), row.get("alfabetizado_avaliado"), row.get("peso_aluno")
    stats["flag_avaliado_diverge_dicionario"] += int(evaluated != expected_evaluated)
    stats["flag_alfabetizado_diverge_dicionario"] += int(alf != expected_alf)
    if evaluated is None:
        stats["avaliacao_nula"] += 1
    if evaluated is not True:
        return
    stats["avaliados"] += 1
    if not isinstance(alf, bool):
        stats["alfabetizacao_nula_entre_avaliados"] += 1
    valid_weight = (isinstance(weight, (int, float)) and not isinstance(weight, bool)
                    and math.isfinite(weight) and weight > 0)
    if not valid_weight:
        stats["peso_invalido_entre_avaliados"] += 1
    if not valid_weight or not isinstance(alf, bool):
        return
    stats["elegiveis_calculo"] += 1
    kahan_add(stats["peso"], float(weight))
    if alf:
        stats["alfabetizados"] += 1
        kahan_add(stats["peso_alfabetizados"], float(weight))


def summarize(items):
    items = list(items)
    denominator = math.fsum(x["peso"][0] for x in items)
    numerator = math.fsum(x["peso_alfabetizados"][0] for x in items)
    return {**{name: sum(x[name] for x in items) for name in COUNTERS},
            "soma_peso_avaliados": denominator, "soma_peso_alfabetizados": numerator,
            "taxa_ponderada": round(100 * numerator / denominator, 4) if denominator else None}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--gold-manifest-uri", default=GOLD_MANIFEST)
    p.add_argument("--output-dir", default="conferencia_pesos_silver")
    p.add_argument("--region", default="us-east-2")
    args = p.parse_args(argv)
    output = Path(args.output_dir)
    if output.exists():
        raise FileExistsError("Choose a new output directory to preserve prior evidence")
    import boto3
    import pyarrow.parquet as pq
    s3 = boto3.client("s3", region_name=args.region)
    manifests, inventory = [], []

    def read_manifest(uri):
        bucket, key = split_s3(uri)
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        manifests.append({"uri": uri, "sha256": hashlib.sha256(raw).hexdigest()})
        return json.loads(raw)

    def parquet_batches(uri, columns, entity):
        bucket, prefix = split_s3(uri)
        objects = sorted((o for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                          for o in page.get("Contents", []) if o["Key"].endswith(".parquet")),
                         key=lambda o: o["Key"])
        if not objects or len(objects) > 100 or sum(o["Size"] for o in objects) > 512 * 1024 * 1024:
            raise ValueError("Unexpected Parquet inventory for " + entity)
        for i, obj in enumerate(objects, 1):
            print(f"Lendo {entity}: arquivo {i}/{len(objects)}", flush=True)
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            parquet = pq.ParquetFile(io.BytesIO(body))
            missing = set(columns) - set(parquet.schema_arrow.names)
            if missing:
                raise ValueError(entity + ": missing " + str(sorted(missing)))
            inventory.append({"entity": entity, "uri": "s3://" + bucket + "/" + obj["Key"],
                              "bytes": len(body), "rows": parquet.metadata.num_rows,
                              "sha256": hashlib.sha256(body).hexdigest()})
            for batch in parquet.iter_batches(batch_size=16384, columns=columns, use_threads=False):
                yield batch.to_pylist()

    gold = read_manifest(args.gold_manifest_uri)
    if gold.get("status") != "SUCCEEDED" or gold.get("all_five_tables_validated") is not True:
        raise ValueError("Use a complete, committed Gold run")
    silver_uris = {x.get("silver_manifest_uri") for x in gold.get("source_tables", {}).values()}
    if len(silver_uris) != 1 or None in silver_uris:
        raise ValueError("Gold must identify exactly one source Silver manifest")
    silver_uri = next(iter(silver_uris))
    silver = read_manifest(silver_uri)
    if silver.get("status") != "SUCCEEDED" or silver.get("layer") != "silver" or silver.get("is_full_batch") is not True:
        raise ValueError("Silver source is not approved")
    entries = {x["table"]: x for x in silver.get("tables", [])}
    for entity in ("alunos", "dicionario", "uf"):
        entry = entries[entity]
        if entry.get("status") != "SUCCEEDED" or entry.get("counts_match") is not True:
            raise ValueError("Unvalidated Silver entity: " + entity)
        if f"/run_id={silver['run_id']}/" not in "/" + split_s3(entry["target_uri"])[1]:
            raise ValueError("Mixed Silver snapshots")
    dictionary = defaultdict(dict)
    for batch in parquet_batches(entries["dicionario"]["target_uri"],
                                 ["id_tabela", "nome_coluna", "chave", "valor"], "dicionario"):
        for row in batch:
            if row["id_tabela"] == "alunos":
                column, key, value = row["nome_coluna"], row["chave"], row["valor"]
                if key in dictionary[column] and dictionary[column][key] != value:
                    raise ValueError("Conflicting dictionary key")
                dictionary[column][key] = value

    groups = defaultdict(new_stats)
    columns = ["ano", "serie", "rede", "id_municipio", "peso_aluno", "avaliado",
               "alfabetizado_avaliado", "presenca", "preenchimento_caderno", "alfabetizado"]
    read_count = 0
    for batch in parquet_batches(entries["alunos"]["target_uri"], columns, "alunos"):
        for row in batch:
            read_count += 1
            state = UF.get(str(row["id_municipio"])[:2], "UF_NAO_IDENTIFICADA")
            key = (row["ano"], row["serie"], row["rede"], state)
            add_row(groups[key], row, dictionary)
    if read_count != entries["alunos"]["target_count"]:
        raise ValueError("Student count differs from the Silver manifest")
    by_network = []
    network_keys = sorted({k[:3] for k in groups}, key=lambda k: tuple(str(v) for v in k))
    for year, series, network in network_keys:
        by_network.append({"ano": year, "serie": series, "rede": network,
            "rede_descricao": dictionary.get("rede", {}).get(network),
            **summarize(v for k, v in groups.items() if k[:3] == (year, series, network))})

    official_states = {}
    for batch in parquet_batches(entries["uf"]["target_uri"],
                                 ["ano", "serie", "rede", "sigla_uf", "taxa_alfabetizacao"], "uf"):
        for row in batch:
            if row["rede"] == "5":
                key = (row["ano"], row["serie"], row["sigla_uf"])
                if key in official_states:
                    raise ValueError("Duplicate official UF key")
                official_states[key] = row["taxa_alfabetizacao"]
    metrics = [x for x in gold["table_metrics"] if x["table"] == "validacao_cruzada_brasil"]
    if len(metrics) != 1:
        raise ValueError("Missing national comparison")
    gold_rows = []
    for batch in parquet_batches(metrics[0]["s3_uri"],
                                 ["ano", "serie", "rede", "taxa_alfabetizacao_calculada",
                                  "taxa_oficial_publicada", "diferenca_pp"], "gold_brasil"):
        gold_rows.extend(batch)
    public = []
    states = []
    # Replicate the CURRENT Gold definition: derived 5 from raw 2/3 plus any
    # raw rows already coded 5. Presence of native 5 is a diagnostic, not proof
    # of duplication. This script does not decide the correct official universe.
    for row in gold_rows:
        year, series = row["ano"], row["serie"]
        selected = {k: v for k, v in groups.items() if k[0] == year and k[1] == series and k[2] in ("2", "3", "5")}
        summary = summarize(selected.values())
        calculated, stored = summary["taxa_ponderada"], row["taxa_alfabetizacao_calculada"]
        official = row["taxa_oficial_publicada"]
        present = sorted({k[3] for k, v in selected.items() if v["elegiveis_calculo"]})
        missing_official = [state for state in present if official_states.get((year, series, state)) is None]
        public.append({"ano": year, "serie": series, **summary,
            "taxa_gold_gravada": stored, "taxa_oficial_publicada": official,
            "diferenca_para_oficial_pp": round(calculated - official, 4) if calculated is not None and official is not None else None,
            "reproduz_taxa_gold_ate_0_0002_pp": abs(calculated - stored) <= 0.0002 if calculated is not None and stored is not None else False,
            "registros_rede_5_nativa": sum(v["registros"] for k, v in selected.items() if k[2] == "5"),
            "ufs_com_microdados_elegiveis": present, "ufs_sem_resultado_oficial_uf": missing_official})
        for state in present:
            data = summarize(v for k, v in selected.items() if k[3] == state)
            ref = official_states.get((year, series, state))
            states.append({"ano": year, "serie": series, "uf_inferida_do_municipio": state,
                **data, "taxa_oficial_uf": ref,
                "diferenca_pp": round(data["taxa_ponderada"] - ref, 4) if ref is not None else None})

    report = {"observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "Read-only S3 audit of the exact committed Silver and Gold snapshots; aggregates only",
        "gold_run_id": gold["run_id"], "silver_run_id": silver["run_id"],
        "manifests": manifests, "alunos_lidos": read_count, "dicionario_alunos": dict(dictionary),
        "por_rede": by_network, "reproducao_publica_gold": public, "por_uf": states,
        "object_inventory": inventory,
        "caveats": ["Reproducing Gold proves implementation agreement, not agreement with official methodology.",
                    "Public network composition remains a business/source definition to verify.",
                    "UF is inferred from the first two digits of the normalized municipality IBGE code.",
                    "No new job, no AWS write, no guessed root cause, no automatic correction."]}
    output.mkdir(parents=True, exist_ok=False)
    target = output / "conferencia_pesos_silver.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print("\nANO | TAXA_RECALCULADA | OFICIAL | DIFERENCA_PP | REPRODUZ_GOLD")
    for row in public:
        print(" | ".join(str(row[k]) for k in ["ano", "taxa_ponderada", "taxa_oficial_publicada",
                                              "diferenca_para_oficial_pp", "reproduz_taxa_gold_ate_0_0002_pp"]))
        print("  Avaliados:", row["avaliados"], "| Soma pesos:", row["soma_peso_avaliados"],
              "| Pesos invalidos:", row["peso_invalido_entre_avaliados"])
        print("  Divergencias de flags:", row["flag_avaliado_diverge_dicionario"], row["flag_alfabetizado_diverge_dicionario"])
        print("  UFs com microdados mas sem resultado UF:", row["ufs_sem_resultado_oficial_uf"])
    print("Relatorio local:", target.resolve())
    print("Nenhum job Glue iniciado; nenhum dado AWS alterado.")
    return report


if __name__ == "__main__":
    main()
