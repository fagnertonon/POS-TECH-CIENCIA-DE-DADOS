"""Compare local official Inep ZIP aggregates against an exported Silver audit.

Reads only local files. Does not connect to AWS or BigQuery.
Does not emit student identifiers or modify pipeline data.
"""
from collections import defaultdict
from datetime import datetime, timezone
import argparse
import hashlib
import json
import math
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

UFS = set("AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO".split())


def sha256(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def inspect_source(path, year, audit):
    expected = {r["uf_inferida_do_municipio"]: r for r in audit["por_uf"] if r["ano"] == year}
    groups = defaultdict(lambda: {"registros": 0, "avaliados": 0, "alfabetizados": 0,
                                  "peso": [], "peso_alfabetizados": []})
    columns = ["SG_UF", "TP_SERIE", "TP_DEPENDENCIA", "IN_PRESENCA_LP", "IN_PREENCHIMENTO_LP",
               "VL_PESO_ALUNO_LP", "IN_ALFABETIZADO"]
    rows_read = 0
    with ZipFile(path) as z:
        with z.open("DADOS/TS_ALUNO.csv") as f:
            for chunk in pd.read_csv(f, sep=";", encoding="latin1", usecols=columns, chunksize=100000):
                rows_read += len(chunk)
                public = chunk.loc[(chunk.TP_SERIE == 2) & chunk.TP_DEPENDENCIA.isin([2, 3])].copy()
                for state, part in public.groupby("SG_UF"):
                    g = groups[state]
                    g["registros"] += len(part)
                    eligible = part.loc[(part.IN_PRESENCA_LP == 1) & (part.IN_PREENCHIMENTO_LP == 1)]
                    g["avaliados"] += len(eligible)
                    weights = pd.to_numeric(eligible.VL_PESO_ALUNO_LP.astype(str).str.replace(",", ".", regex=False), errors="raise")
                    if not weights.map(lambda x: math.isfinite(x) and x > 0).all():
                        raise ValueError("Invalid evaluated weight in official source")
                    if not eligible.IN_ALFABETIZADO.isin([0, 1]).all():
                        raise ValueError("Unknown literacy flag in official source")
                    alphabetized = eligible.IN_ALFABETIZADO == 1
                    g["alfabetizados"] += int(alphabetized.sum())
                    g["peso"].append(float(weights.sum()))
                    g["peso_alfabetizados"].append(float(weights.loc[alphabetized].sum()))
        with z.open("DADOS/TS_ESTADO.csv") as f:
            states = pd.read_csv(f, sep=";", encoding="latin1")
        state_columns = list(states.columns)
        if "ID_TIPO_REDE" in states:
            published = states.loc[(states.TP_SERIE == 2) & (states.ID_TIPO_REDE == 5)]
        elif "rede" in states:
            published = states.loc[(states.serie == 2) & (states.rede == 5)]
        else:
            raise ValueError(f"Unexpected TS_ESTADO schema: {state_columns}")
        state_column = "SG_UF" if "SG_UF" in published else "sigla_uf"
        official_ufs = sorted(published[state_column].tolist())
    result = []
    for state, g in sorted(groups.items()):
        denominator, numerator = math.fsum(g.pop("peso")), math.fsum(g.pop("peso_alfabetizados"))
        rate = round(100 * numerator / denominator, 4)
        ref = expected.get(state)
        checks = {k: ref is not None and g[k] == ref[k] for k in ["registros", "avaliados", "alfabetizados"]}
        checks.update({"soma_pesos": ref is not None and math.isclose(denominator, ref["soma_peso_avaliados"], rel_tol=0, abs_tol=0.000001),
                       "soma_pesos_alfabetizados": ref is not None and math.isclose(numerator, ref["soma_peso_alfabetizados"], rel_tol=0, abs_tol=0.000001),
                       "taxa": ref is not None and rate == ref["taxa_ponderada"]})
        result.append({"uf": state, **g, "soma_pesos": denominator,
                       "soma_pesos_alfabetizados": numerator, "taxa": rate, "comparacao_silver": checks})
    denominator = math.fsum(r["soma_pesos"] for r in result)
    numerator = math.fsum(r["soma_pesos_alfabetizados"] for r in result)
    return {"ano": year, "official_zip": str(path.resolve()), "official_zip_sha256": sha256(path),
            "url": f"https://download.inep.gov.br/dados_abertos/microdados_avaliacao_da_alfabetizacao_{year}.zip",
            "rows_read": rows_read, "ufs_alunos_publicos": sorted(groups),
            "ufs_ausentes_alunos_publicos": sorted(UFS - set(groups)),
            "ufs_ts_estado_rede_publica": official_ufs,
            "ufs_alunos_sem_ts_estado": sorted(set(groups) - set(official_ufs)),
            "silver_ufs_without_source_match": sorted(set(expected) - set(groups)),
            "all_compared_aggregates_match": set(expected) == set(groups) and all(all(r["comparacao_silver"].values()) for r in result),
            "recalculated_public_rate": round(100 * numerator / denominator, 4),
            "por_uf": result,
            "scope_limit": "Agreement of counts and weighted aggregates is not a row-by-row identity check or proof of official national methodology."}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--audit", type=Path, required=True)
    p.add_argument("--source-dir", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise FileExistsError("Preserve existing evidence; choose a new output path")
    audit = json.loads(args.audit.read_text(encoding="utf-8"))
    result = {"observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "audit_sha256": sha256(args.audit), "gold_run_id": audit["gold_run_id"],
              "silver_run_id": audit["silver_run_id"],
              "years": [inspect_source(args.source_dir / f"microdados_avaliacao_da_alfabetizacao_{year}.zip", year, audit) for year in [2023, 2024]]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    for r in result["years"]:
        print(r["ano"], "rows=", r["rows_read"], "ufs=", len(r["ufs_alunos_publicos"]),
              "absent=", r["ufs_ausentes_alunos_publicos"], "rate=", r["recalculated_public_rate"],
              "aggregates_match_silver=", r["all_compared_aggregates_match"])
    print(args.output.resolve())


if __name__ == "__main__":
    main()
