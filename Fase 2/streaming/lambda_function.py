"""SQS -> Bronze -> Silver -> Gold de simulação, com identidade (run_id,event_id).

event_time é o momento de emissão do replay simulado, não o ano educacional.
Cada evento produz um JSON na Bronze e um claim de idempotência na auditoria.
O claim inclui o envelope integral para recuperar uma projeção interrompida.
Derivados incrementais gzip são imutáveis. finalize_simulation.py compacta um
conjunto fechado em Parquet Snappy; nunca soma snapshots ou dados oficiais.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import gzip
import json
import math
import os
import re
import time


ALL_ENTITIES = frozenset({
    "alunos", "municipio", "uf", "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "dicionario",
})
SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,160}\Z")
UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)\Z")
PIPELINE = "fiap-inep-streaming"
NAMESPACE = "FIAP/Streaming"
PIPELINE_VERSION = "2.0.0"
ORIGIN = "simulacao_replay_inep"
UF_BY_IBGE = dict(zip(
    "11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 41 42 43 50 51 52 53".split(),
    "RO AC AM RR PA AP TO MA PI CE RN PB PE AL SE BA MG ES RJ SP PR SC RS MS MT GO DF".split(),
))
_S3 = None


class InvalidEnvelope(ValueError):
    pass


class EventIdentityConflict(ValueError):
    """A identidade já está associada a outro envelope; nunca sobrescrever."""


class StoredEventCorruption(ValueError):
    pass


class InvalidPayload(ValueError):
    """Bronze preservada; qualidade bloqueia publicação analítica."""


@dataclass(frozen=True)
class Config:
    bucket: str
    prefix: str
    idempotency_prefix: str
    allowed_entities: frozenset[str]
    silver_prefix: str = "silver/inep_streaming"
    gold_prefix: str = "gold/inep_streaming"
    processed_prefix: str = "auditoria/streaming/processed"

    def __post_init__(self):
        # Configuração não pode redirecionar a simulação aos datasets oficiais.
        for value, root in ((self.prefix, "bronze/inep_streaming"),
                            (self.silver_prefix, "silver/inep_streaming"),
                            (self.gold_prefix, "gold/inep_streaming"),
                            (self.idempotency_prefix, "auditoria/streaming/idempotency"),
                            (self.processed_prefix, "auditoria/streaming/processed")):
            if _prefix(value) != root:
                raise ValueError("Only isolated simulation prefixes are allowed")


def _prefix(value):
    value = value.strip("/")
    if not value or any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError("Invalid S3 prefix")
    if not re.fullmatch(r"[A-Za-z0-9_./=-]+", value):
        raise ValueError("Invalid S3 prefix")
    return value


def load_config():
    bucket = os.environ.get("BUCKET", "fiap-tc-fase2-2026")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid BUCKET")
    prefix = _prefix(os.environ.get("PREFIX", "bronze/inep_streaming"))
    identity_prefix = _prefix(os.environ.get("IDEMPOTENCY_PREFIX", "auditoria/streaming/idempotency"))
    if prefix == identity_prefix or prefix.startswith(identity_prefix + "/") or identity_prefix.startswith(prefix + "/"):
        raise ValueError("Bronze and idempotency prefixes must be separate")
    entities = frozenset(item.strip() for item in os.environ.get("ALLOWED_ENTITIES", "alunos").split(","))
    if not entities or not entities.issubset(ALL_ENTITIES):
        raise ValueError("Invalid ALLOWED_ENTITIES")
    if entities != frozenset({"alunos"}):
        raise ValueError("Analytical simulation supports only alunos")
    return Config(bucket, prefix, identity_prefix, entities,
                  _prefix(os.environ.get("SILVER_PREFIX", "silver/inep_streaming")),
                  _prefix(os.environ.get("GOLD_PREFIX", "gold/inep_streaming")),
                  _prefix(os.environ.get("PROCESSED_PREFIX", "auditoria/streaming/processed")))


def utc_now():
    return datetime.now(timezone.utc)


def iso_utc(value):
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical_json(value):
    # allow_nan=False é essencial: NaN/Infinity não pertencem ao contrato JSON.
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha256(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _reject_constant(_value):
    raise InvalidEnvelope("Non-finite JSON number")


def parse_event(body, config):
    if not isinstance(body, str):
        raise InvalidEnvelope("SQS body must be text")
    try:
        envelope = json.loads(body, parse_constant=_reject_constant)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise InvalidEnvelope("Body is not JSON") from exc
    if not isinstance(envelope, dict):
        raise InvalidEnvelope("Envelope must be an object")
    if type(envelope.get("schema_version")) is not int or envelope["schema_version"] != 1:
        raise InvalidEnvelope("Unsupported schema_version")
    for field in ("event_id", "run_id", "source_snapshot_run_id"):
        if not isinstance(envelope.get(field), str) or not SAFE_ID.fullmatch(envelope[field]):
            raise InvalidEnvelope(f"Invalid {field}")
    if envelope.get("entity") not in config.allowed_entities:
        raise InvalidEnvelope("Unsupported entity")
    if envelope.get("source") != "inep_bronze_replay" or envelope.get("simulation") is not True:
        raise InvalidEnvelope("Expected an explicitly simulated INEP Bronze replay")
    if not isinstance(envelope.get("payload"), dict):
        raise InvalidEnvelope("payload must be an object")
    stamp = envelope.get("event_time")
    if not isinstance(stamp, str) or not UTC_TIMESTAMP.fullmatch(stamp):
        raise InvalidEnvelope("event_time must be an ISO UTC timestamp")
    try:
        event_time = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidEnvelope("Invalid event_time") from exc
    if event_time.utcoffset() != timedelta(0):
        raise InvalidEnvelope("event_time must use UTC")
    # Também valida números aninhados que eventualmente excedam o JSON finito.
    try:
        canonical_json(envelope)
    except (ValueError, TypeError, OverflowError) as exc:
        raise InvalidEnvelope("Envelope is not finite JSON") from exc
    return envelope, event_time


def _code(exc):
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _is_conflict(exc):
    return _code(exc) in {"PreconditionFailed", "ConditionalRequestConflict", "412", "409"}


def _get_stored(s3, config, key, expected_hash):
    result = s3.get_object(Bucket=config.bucket, Key=key)
    try:
        stored = json.loads(result["Body"].read(), parse_constant=_reject_constant)
        stored_hash = sha256(stored["event"])
        declared_hash = stored["ingestion"]["event_sha256"]
        if stored_hash != declared_hash:
            raise StoredEventCorruption("Stored event hash mismatch")
        if stored_hash != expected_hash:
            raise EventIdentityConflict("Event identity already contains another envelope")
    except (KeyError, json.JSONDecodeError, TypeError) as exc:
        raise StoredEventCorruption("Invalid stored event") from exc
    return stored


def _put_once(s3, config, key, value):
    s3.put_object(
        Bucket=config.bucket, Key=key, Body=canonical_json(value) + b"\n",
        ContentType="application/json", IfNoneMatch="*",
        Metadata={"event-sha256": value["ingestion"]["event_sha256"]},
    )


def stage_paths(envelope, config):
    day = datetime.fromisoformat(envelope["event_time"].replace("Z", "+00:00")).date().isoformat()
    suffix = f"event_date={day}/run_id={envelope['run_id']}/event_id={envelope['event_id']}"
    return {
        "bronze_key": f"{config.prefix}/entity=alunos/{suffix}.json",
        "silver_key": f"{config.silver_prefix}/entity=alunos/{suffix}.json.gz",
        "gold_key": f"{config.gold_prefix}/contribuicoes/{suffix}.json.gz",
        "receipt_key": f"{config.processed_prefix}/run_id={envelope['run_id']}/event_id={envelope['event_id']}.json",
    }


def transform_payload(payload):
    """Domínios 0/1 confirmados no dicionário do snapshot Silver de 26/10/2026.

    Não infere alfabetização pela proficiência e não imputa pesos ausentes.
    A taxa da amostra é uma simulação, sem comparação com a taxa oficial.
    """
    required = {"ano", "id_municipio", "id_aluno", "id_escola", "serie", "rede",
                "presenca", "preenchimento_caderno", "alfabetizado", "peso_aluno"}
    if not required.issubset(payload):
        raise InvalidPayload("Missing source columns")
    year = payload["ano"]
    if type(year) is not int or not 2000 <= year <= 2100:
        raise InvalidPayload("Invalid educational year")
    municipality = payload["id_municipio"]
    if not isinstance(municipality, str) or not re.fullmatch(r"\d{7}", municipality) or municipality[:2] not in UF_BY_IBGE:
        raise InvalidPayload("Municipality does not match an IBGE UF")
    for name in ("id_aluno", "id_escola"):
        if not isinstance(payload[name], str) or not re.fullmatch(r"\d{1,20}", payload[name]):
            raise InvalidPayload("Invalid source identifier")
    if not isinstance(payload["rede"], str) or payload["rede"] not in {"1", "2", "3", "4"} or payload["serie"] != "2":
        raise InvalidPayload("Expected elementary network and second year cohort")
    for name in ("presenca", "preenchimento_caderno", "alfabetizado"):
        if payload[name] is not None and (not isinstance(payload[name], str) or payload[name] not in {"0", "1"}):
            raise InvalidPayload("Unknown dictionary category")
    present = {"1": True, "0": False}.get(payload["presenca"])
    filled = {"1": True, "0": False}.get(payload["preenchimento_caderno"])
    # SQL three-valued AND: false dominates null, as in the official Silver.
    evaluated = False if present is False or filled is False else (True if present is True and filled is True else None)
    if evaluated is None:
        raise InvalidPayload("Unknown evaluation status")
    literate = {"1": True, "0": False}.get(payload["alfabetizado"]) if evaluated else None
    weight = payload["peso_aluno"]
    if evaluated:
        try:
            valid_weight = type(weight) in (int, float) and math.isfinite(weight) and weight > 0
        except (OverflowError, TypeError, ValueError):
            valid_weight = False
        if literate is None or not valid_weight:
            raise InvalidPayload("Evaluated pupil requires known literacy and positive finite weight")
    silver = dict(payload, sigla_uf=UF_BY_IBGE[municipality[:2]], presente=present,
                  caderno_preenchido=filled, avaliado=evaluated, alfabetizado_avaliado=literate)
    gold = {"ano": year, "sigla_uf": silver["sigla_uf"], "serie": payload["serie"], "rede": payload["rede"],
            "eventos": 1, "alunos_avaliados": int(evaluated), "alunos_alfabetizados": int(literate is True),
            "soma_peso_avaliados": float(weight) if evaluated else 0.0,
            "soma_peso_alfabetizados": float(weight) if literate is True else 0.0}
    return silver, gold


def _persist_derived(s3, config, key, value, compressed=False, volatile=()):
    raw = canonical_json(value) + b"\n"
    body = gzip.compress(raw, mtime=0) if compressed else raw
    try:
        s3.put_object(Bucket=config.bucket, Key=key, Body=body, IfNoneMatch="*",
                      ContentType="application/json", **({"ContentEncoding": "gzip"} if compressed else {}))
        return value, True
    except Exception as exc:
        if not _is_conflict(exc):
            raise
        raw = s3.get_object(Bucket=config.bucket, Key=key)["Body"].read()
        existing = json.loads(gzip.decompress(raw) if compressed else raw, parse_constant=_reject_constant)
        # Dados determinísticos devem coincidir integralmente. Só o horário da
        # primeira conclusão varia numa corrida entre dois consumidores.
        stable = lambda obj: {k: v for k, v in obj.items() if k not in volatile}
        if canonical_json(stable(existing)) != canonical_json(stable(value)):
            raise StoredEventCorruption("Derived object differs from deterministic projection")
        return existing, False


def project_analytics(s3, config, envelope, event_hash):
    paths = stage_paths(envelope, config)
    try:
        silver, gold = transform_payload(envelope["payload"])
    except InvalidPayload:
        # Persistir evidência bloqueante sem body/identificadores pessoais.
        failure = {"schema_version": 2, "pipeline_version": PIPELINE_VERSION, "origin": ORIGIN,
                   "simulation": True, "status": "QUALITY_REJECTED", "event_sha256": event_hash,
                   "run_id": envelope["run_id"], "event_id": envelope["event_id"],
                   "bronze_key": paths["bronze_key"], "rule": "valid_analytical_payload"}
        key = paths["receipt_key"].replace("/event_id=", "/rejected_event_id=")
        _persist_derived(s3, config, key, failure)
        raise
    provenance = {k: envelope[k] for k in ("run_id", "event_id", "source_snapshot_run_id", "event_time")}
    provenance["bronze_key"] = paths["bronze_key"]
    for layer, data, key in (("silver", silver, paths["silver_key"]), ("gold_contribution", gold, paths["gold_key"])):
        derived = {"schema_version": 2, "pipeline_version": PIPELINE_VERSION, "layer": layer,
                   "origin": ORIGIN, "simulation": True, "event_sha256": event_hash,
                   "data_sha256": sha256(data), "provenance": provenance, "data": data}
        _persist_derived(s3, config, key, derived, compressed=True)
    # Instante lido DEPOIS do PUT Gold: latência até publicação analítica real.
    completed = utc_now()
    emitted = datetime.fromisoformat(envelope["event_time"].replace("Z", "+00:00"))
    receipt = {"schema_version": 2, "pipeline_version": PIPELINE_VERSION, "status": "GOLD_READY",
               "origin": ORIGIN, "simulation": True, "event_sha256": event_hash,
               **{k: envelope[k] for k in ("run_id", "event_id", "source_snapshot_run_id", "event_time")},
               **{k: v for k, v in paths.items() if k != "receipt_key"},
               "silver_data_sha256": sha256(silver), "gold_data_sha256": sha256(gold),
               "completed_at_utc": iso_utc(completed),
               "end_to_end_latency_seconds": max(0.0, (completed - emitted).total_seconds())}
    saved, new = _persist_derived(s3, config, paths["receipt_key"], receipt,
                                 volatile=("completed_at_utc", "end_to_end_latency_seconds"))
    recorded_time = datetime.fromisoformat(saved["completed_at_utc"].replace("Z", "+00:00"))
    if not math.isclose(saved["end_to_end_latency_seconds"], max(0.0, (recorded_time-emitted).total_seconds()), abs_tol=0.000001):
        raise StoredEventCorruption("Invalid persisted end-to-end latency")
    return saved, new


def process_record(record, context, config, s3):
    envelope, event_time = parse_event(record["body"], config)
    run_id, event_id = envelope["run_id"], envelope["event_id"]
    event_hash = sha256(envelope)
    target_key = (
        f"{config.prefix}/entity={envelope['entity']}/event_date={event_time.date().isoformat()}"
        f"/run_id={run_id}/event_id={event_id}.json"
    )
    claim_key = f"{config.idempotency_prefix}/run_id={run_id}/event_id={event_id}.json"
    received = utc_now()
    if (event_time - received).total_seconds() > 5:
        raise InvalidEnvelope("event_time is more than five seconds in the future")
    first_received = iso_utc(received)
    stored = {
        "event": envelope,
        "ingestion": {
            "received_at_utc": first_received,
            "claim_started_at_utc": first_received,
            "sqs_message_id": record["messageId"],
            "lambda_request_id": getattr(context, "aws_request_id", "unknown"),
            "event_sha256": event_hash,
            "payload_sha256": sha256(envelope["payload"]),
            "target_key": target_key,
        },
    }
    try:
        _put_once(s3, config, claim_key, stored)
    except Exception as exc:
        if not _is_conflict(exc):
            raise
        # S3 GET é fortemente consistente. Em 409 sem objeto visível, propagar
        # a falha: a fila fará retry; nunca confirmar um evento que não foi salvo.
        stored = _get_stored(s3, config, claim_key, event_hash)
        if stored["ingestion"].get("target_key") != target_key:
            raise StoredEventCorruption("Claim target path mismatch")

    try:
        # Sempre tentar o PUT condicional, inclusive ao recuperar um claim.
        # Sem s3:ListBucket, um GET de chave inexistente pode retornar 403 em
        # vez de 404; assim a recuperação não depende dessa permissão ampla.
        _put_once(s3, config, target_key, stored)
    except Exception as exc:
        if not _is_conflict(exc):
            raise
        stored = _get_stored(s3, config, target_key, event_hash)
    # Duplicata Bronze pode ter etapas analíticas faltantes: sempre completar.
    receipt, projected = project_analytics(s3, config, envelope, event_hash)
    stored = dict(stored, analytical_receipt=receipt)
    return ("ACCEPTED" if projected else "DUPLICATE"), stored, (receipt["end_to_end_latency_seconds"] if projected else None)


def _client():
    global _S3
    if _S3 is None:
        import boto3
        _S3 = boto3.client("s3")
    return _S3


def _emit_metrics(counts, latencies, invocation_id):
    def emit(values, include_counts):
        definitions = []
        payload = {"kind": "STREAMING_BATCH" if include_counts else "STREAMING_LATENCY", "Pipeline": PIPELINE,
                   "pipeline_version": PIPELINE_VERSION, "latency_target": "gold_contribution", "invocation_id": invocation_id}
        if include_counts:
            payload.update(counts)
            definitions.extend({"Name": name, "Unit": "Count"} for name in counts)
        if values:
            payload["LatencySeconds"] = [round(value, 6) for value in values]
            definitions.append({"Name": "LatencySeconds", "Unit": "Seconds"})
        payload["_aws"] = {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{"Namespace": NAMESPACE, "Dimensions": [["Pipeline"]], "Metrics": definitions}],
        }
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False))
    emit(latencies[:100], True)
    for index in range(100, len(latencies), 100):
        emit(latencies[index:index + 100], False)


def lambda_handler(event, context):
    records = event.get("Records")
    if not isinstance(records, list) or any(not isinstance(record, dict) or not isinstance(record.get("messageId"), str) or not record["messageId"] for record in records):
        raise ValueError("Expected an SQS batch with message IDs")
    config, s3 = load_config(), _client()
    failures, latencies = [], []
    counts = {"Received": len(records), "Accepted": 0, "Duplicate": 0, "Failed": 0}
    for record in records:
        try:
            status, stored, latency = process_record(record, context, config, s3)
            counts["Accepted" if status == "ACCEPTED" else "Duplicate"] += 1
            if latency is not None:
                latencies.append(latency)
            print(json.dumps({
                "kind": "STREAMING_EVENT", "status": status,
                "run_id": stored["event"]["run_id"], "event_id": stored["event"]["event_id"],
                "sqs_message_id": record["messageId"], "target_key": stored["ingestion"]["target_key"],
                "gold_key": stored["analytical_receipt"]["gold_key"], "pipeline_version": PIPELINE_VERSION,
                "gold_completed_at_utc": stored["analytical_receipt"]["completed_at_utc"],
            }, separators=(",", ":")))
        except Exception as exc:
            counts["Failed"] += 1
            failures.append({"itemIdentifier": record["messageId"]})
            # Não registrar body, payload ou mensagens de erro de terceiros.
            print(json.dumps({
                "kind": "STREAMING_EVENT", "status": "FAILED",
                "sqs_message_id": record["messageId"], "error_type": type(exc).__name__,
            }, separators=(",", ":")))
    _emit_metrics(counts, latencies, getattr(context, "aws_request_id", "unknown"))
    return {"batchItemFailures": failures}
