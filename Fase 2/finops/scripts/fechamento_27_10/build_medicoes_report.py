"""Local three-page addendum from persisted read-only FinOps evidence.

No cloud/network call, extraction, billing change, email or alteration of the
previous forecast. The PDF authoring marker ran once for this new operation.
"""
from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from xml.sax.saxutils import escape


BASE = Path(__file__).resolve().parents[1]
METRICS_DOC = "https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CloudWatch-Logs-Monitoring-CloudWatch-Metrics.html"
STOCK_DOC = "https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_LogGroup.html"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def number(value, digits=0):
    return f"{Decimal(str(value)):,.{digits}f}".replace(",", "#").replace(".", ",").replace("#", ".")


def prepare_gcp_tables(raw_path):
    """Prepare measured metadata/telemetry separately from any billed-cost row.

    Used only after the reviewed GCP closure and consolidated state are ready.
    Source capacity and API response-size distributions are never silently
    renamed to historical Storage Read billed bytes or network egress bytes.
    """
    raw = read(raw_path)
    assert raw["read_only"] is True and raw["api_calls"] == 19 and raw["errors"] == []
    assert raw["starts_sql_queries"] is False and raw["starts_storage_read_sessions"] is False
    assert raw["changes_billing_apis_iam"] is False
    metadata = []
    for table in raw["source_table_metadata"]["tables"]:
        assert table["error"] is None
        values = table["metadata"]
        metadata.append({"table": table["table"], "rows": int(values["numRows"]),
                         "logical_bytes": int(values["numTotalLogicalBytes"]),
                         "location": values["location"], "last_modified_ms": values["lastModifiedTime"]})
    assert len(metadata) == 7 and len({row["table"] for row in metadata}) == 7
    by_type = {series["metric_type"]: series for series in raw["time_series"]}
    counts = by_type["serviceruntime.googleapis.com/api/request_count"]
    responses = by_type["serviceruntime.googleapis.com/api/response_sizes"]
    assert counts["complete"] is True and responses["complete"] is True
    telemetry = []
    for name in ("CreateReadSession", "ReadRows"):
        method = "google.cloud.bigquery.storage.v1.BigQueryRead." + name
        requests = Decimal(counts["by_method"][method]["delta_sum"])
        size = Decimal(responses["by_method"][method]["delta_sum"])
        telemetry.append({"method": name, "requests": str(requests),
                          "response_size_distribution_sum_bytes": str(size),
                          "billing_unit_proven": False})
    return {"source_sha256": sha(raw_path), "window": raw["window"],
            "observed_at_utc": raw["observed_at_utc"], "tables": metadata,
            "total_rows": sum(row["rows"] for row in metadata),
            "total_logical_bytes": sum(row["logical_bytes"] for row in metadata),
            "telemetry": telemetry, "requires_semantic_review": True}


def load():
    paths = {name: BASE / name for name in ("estado_pendencias.json", "fechamento_logs.json",
             "raw/aws_logs/log_growth_readonly.json", "logs_T4.json", "auditoria_enunciado.json",
             "fechamento_gcp.json", "raw/gcp_existing_metrics.json", "modelo_finops.json")}
    state, logs, audit = (read(paths[name]) for name in ("estado_pendencias.json", "fechamento_logs.json", "auditoria_enunciado.json"))
    assignment = Path(audit["sources"]["assignment_pdf"]["path"])
    assert sha(assignment) == state["requirements"]["source_pdf_sha256"]
    assert sha(paths["fechamento_logs.json"]) == state["new_log_measurement"]["sha256"]
    assert sha(paths["raw/aws_logs/log_growth_readonly.json"]) == state["new_log_measurement"]["raw_sha256"]
    model_path = Path(audit["sources"]["model"]["path"])
    assert sha(model_path) == state["prior_forecast"]["sha256"]
    assert state["project_ceiling_total_usd"] == 140
    assert state["prior_forecast"]["complete_total_usd"] is None
    assert state["completion"]["closed_total_estimate"] is False
    assert state["completion"]["unavailable_quantities_assigned_zero"] is False
    assert state["gcp"]["new_script_executed"] is True and state["gcp"]["credential_grant_performed"] is True
    assert state["gcp"]["permission_question_pending"] is False
    assert sha(paths["raw/gcp_existing_metrics.json"]) == state["gcp"]["collection_sha256"]
    assert sha(paths["modelo_finops.json"]) == state["forecast_update"]["sha256"]
    current_model = read(paths["modelo_finops.json"])
    assert current_model["calculated_reference"]["complete_total_usd"] is None
    gcp = read(paths["fechamento_gcp.json"])
    assert gcp["status"] == "STORAGE_READ_PROSPECTIVE_LINE_CLOSABLE"
    assert gcp["reconciliation"]["all_seven_compatible"] is True
    assert gcp["reconciliation"]["total_source_rows"] == 3902954
    assert gcp["reconciliation"]["logical_bytes"] == 271547681
    assert gcp["storage_read_projection"]["historical_actual_scanned_bytes"] is None
    assert gcp["egress"]["billed_network_bytes"] is None
    projection = gcp["storage_read_projection"]
    expected_read = Decimal(271547681) / Decimal(2**40) * 4 * Decimal("1.10")
    assert abs(expected_read - Decimal(projection["gross_usd_30d"])) < Decimal("1e-25")
    assert state["forecast_update"]["unmeasured_cost_rows"] == 2
    validation = logs["collection_validation"]
    assert validation["metric_queries"] == validation["metric_series"] == 14
    assert validation["all_series_api_complete"] is True and validation["ingestion_pagination_complete"] is True
    assert validation["api_errors"] == 0 and validation["omitted_log_groups"] == []
    measured = logs["ingestion_measured"]
    assert measured["totals"]["IncomingBytes"]["sum_of_reported_datapoints"] == 6216753
    assert measured["totals"]["IncomingLogEvents"]["sum_of_reported_datapoints"] == 27113
    assert measured["totals"]["IncomingBytes"]["missing_groups_assigned_zero"] is False
    assert logs["archive_growth_decision"]["quantity"] is None
    assert logs["reported_stock"]["current_bytes"] == 7417226
    stock_cost = Decimal(7417226) / Decimal(2 ** 30) * Decimal("0.03")
    assert abs(stock_cost - Decimal(state["new_log_measurement"]["stock_cost_normalized_month_usd"])) < Decimal("1e-25")
    fee = state["measurement_reference_cost"]
    assert Decimal(fee["quantity_metrics_requested"]) * Decimal(fee["unit_price_usd"]) == Decimal(fee["one_off_reference_usd"])
    paths["assignment_pdf"] = assignment
    paths["prior_model"] = model_path
    return {"state": state, "logs": logs, "audit": audit, "gcp": gcp, "paths": paths,
            "hashes": {name: sha(path) for name, path in paths.items()}, "stock_cost": stock_cost}


def build(model, output):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak
    from pypdf import PdfReader
    for name, filename in (("AddArial", "arial.ttf"), ("AddBold", "arialbd.ttf")):
        pdfmetrics.registerFont(TTFont(name, str(Path("C:/Windows/Fonts") / filename)))
    page = landscape(A4)
    width, margin = page[0] - 76, 38
    navy, teal, ink, muted, pale, line = map(colors.HexColor,
        ["#17324D", "#087E8B", "#253748", "#5B6D7D", "#EDF4F7", "#CCDCE5"])
    body = ParagraphStyle("addbody", fontName="AddArial", fontSize=10.1, leading=14.1, textColor=ink, spaceAfter=7)
    styles = {"body": body,
        "title": ParagraphStyle("addtitle", parent=body, fontName="AddBold", fontSize=21, leading=25, textColor=navy, spaceAfter=9, keepWithNext=True),
        "h": ParagraphStyle("addh", parent=body, fontName="AddBold", fontSize=11.3, leading=15, textColor=teal, spaceBefore=5, spaceAfter=6, keepWithNext=True),
        "small": ParagraphStyle("addsmall", parent=body, fontSize=8.3, leading=11.3, textColor=muted, spaceAfter=5),
        "cell": ParagraphStyle("addcell", parent=body, fontSize=8.7, leading=11.5, spaceAfter=0),
        "head": ParagraphStyle("addhead", parent=body, fontName="AddBold", fontSize=8.6, leading=11.5, textColor=colors.white, spaceAfter=0)}
    story, txt = [], []
    def p(text, style="body"):
        txt.append(str(text))
        return Paragraph(escape(str(text)).replace("\n", "<br/>"), styles[style])
    def text(value, style="body"): story.append(p(value, style))
    def heading(value): text(value, "h")
    def start(title, label):
        if story: story.append(PageBreak())
        text("FIAP FASE 2 | ADENDO FINOPS | MEDIÇÕES DE 27/10/2026", "small")
        text(title, "title")
        text(label, "small")
    def table(headers, rows, fractions):
        data = [[p(value, "head") for value in headers]] + [[p(value, "cell") for value in row] for row in rows]
        view = Table(data, colWidths=[width * value for value in fractions], repeatRows=1, hAlign="LEFT")
        view.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), navy),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, pale]),
            ("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 7),
            ("RIGHTPADDING", (0, 0), (-1, -1), 7), ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6), ("LINEBELOW", (0, 0), (-1, 0), 0.8, teal),
            ("LINEBELOW", (0, -1), (-1, -1), 0.5, line)]))
        story.extend([view, Spacer(1, 7)])
    state, logs = model["state"], model["logs"]

    gcp = model["gcp"]
    projection = gcp["storage_read_projection"]
    start("Adendo FinOps: nova base medida e duas lacunas", "1 DE 3 | SOMENTE METADADOS E TELEMETRIA; SEM NOVAS EXTRAÇÕES")
    text("A renovação de acesso GCP foi aprovada pelo usuário e concluída no CloudShell oficial. Uma coleta limitada fez 19 chamadas, sem erros, lendo metadados e telemetria existentes; Monitoring respondeu. Não iniciou SQL, ReadRows ou novas sessões Storage Read, nem ativou billing/API ou alterou IAM. O metadado atual de billing continua false; isso não prova consumo histórico zero.")
    table(["Serviço", "Unidade", "Quantidade medida / base", "Preço unitário", "Custo projetado (30d)"], [
        ["BigQuery Storage Read - projeção integral", "TiB de capacidade lógica modelada", "271.547.681 B/leitura; 4 leituras = " + number(projection["modeled_TiB_30d"], 15) + " TiB", "US$ 1,10 / TiB (referência)", "US$ " + number(projection["gross_usd_30d"], 12)],
        ["CloudWatch Logs - crescimento do arquivo", "GB-mês de incremento", "n.a. - incremento comprimido por ciclo não medido", "US$ 0,03 / GB-mês", "n.a."],
        ["Saída GCP para AWS", "GiB de saída faturável", "n.a. - bytes elegíveis ao SKU de rede não comprovados", "US$ 0,12 / GiB (referência)", "n.a."],
        ["CloudWatch Logs - estoque já incluído", "GB-mês de base", "7.417.226 bytes / 2^30 = 0,006907830015 GB", "US$ 0,03 / GB-mês", "US$ " + number(model["stock_cost"], 12)],
    ], [0.24, 0.16, 0.30, 0.15, 0.15])
    text("Storage Read é uma estimativa prospectiva: mesmas sete tabelas, linhas e colunas; uma leitura completa por batch, quatro vezes em 30 dias. Não representa bytes historicamente faturados, nem inclui retries, leituras extras ou prévias. A franquia publicada de 300 TiB/mês por conta de billing não foi aplicada; sua disponibilidade não foi medida e ela não cobre egress. Uma atualização da fonte exige nova medição de capacidade.", "small")
    heading("Novo subtotal parcial: US$ " + number(state["forecast_update"]["available_partial_subtotal_usd"], 9))
    text("A parcela Storage Read acrescenta US$ " + number(projection["gross_usd_30d"], 12) + " ao subtotal anterior: agora são 19 parcelas calculadas e duas n.a. O estoque de logs já estava incluído; não somá-lo novamente, nem repetir um replay já contido na base. O custo completo continua indisponível; a base histórica de requests também não representa isoladamente quatro novos ciclos.")
    text("US$ 140 é o teto TOTAL, sem renovação mensal. Quatro batch e quatro replays são hipótese do grupo, não frequência do enunciado. US$ 136,59 é a fotografia de créditos das 17:18 UTC, sem nova conferência nesta etapa. Não subtrair o uso outra vez dos créditos. Tarifas são referências brutas, antes de franquias e créditos.", "small")

    start("GCP: capacidade medida e telemetria distinta", "2 DE 3 | COLETA DE 27/10/2026 18:16 UTC; PROJETO fiap1-512214")
    text("As sete tabelas US somam 3.902.954 linhas e 271.547.681 bytes lógicos atuais. As linhas e quantidades de colunas conferem com a fonte registrada na Bronze; as últimas modificações antecedem a ingestão. Os hashes de schema são referências atuais: não se afirmou igualdade de hash histórico entre as serializações Spark e BigQuery.", "small")
    table(["Tabela", "Linhas medidas", "Colunas", "Bytes lógicos medidos"], [[
        row["table"], number(row["metadata_rows"]), str(row["metadata_column_count"]), number(row["measured_logical_bytes"])]
        for row in gcp["reconciliation"]["tables"]], [0.40, 0.20, 0.12, 0.28])
    heading("Telemetria histórica de respostas API, não egress faturável")
    table(["Método", "Requests observados", "Soma da distribuição response_sizes"], [
        ["CreateReadSession", "16", "aproximadamente 43.650 bytes"],
        ["ReadRows", "13", "1.860.997 bytes observados"],
    ], [0.36, 0.24, 0.40])
    text("Janela: 20/10, de 18:20 a 18:30 UTC; contém batch e prévias, sem isolamento de um run Glue. response_sizes é uma distribuição registrada por conclusão de request. Não se comprovou equivalência com todas as mensagens de streaming ou com bytes de um SKU de rede. Bytes lógicos e Parquet comprimido também não substituem egress. A auditoria consultada retornou zero registros; isso não foi convertido em consumo zero.", "small")
    text("A recusa inicial do clique em Autorizar foi resolvida por aprovação humana explícita e renovação no CloudShell oficial. Não há autorização pendente neste fechamento. O coletor executou uma vez, sem reextração; o estado de billing foi reconferido, mas não alterado.", "small")

    start("Logs, critérios do desafio e próximos passos", "3 DE 3 | EGRESS E INCREMENTO DO ARQUIVO CONTINUAM N.A.")
    text("Logs AWS, 27/10 de 00:00 até 18:00 UTC exclusivo: 6.216.753 bytes IncomingBytes sem compressão e 27.113 eventos. As 14 séries tiveram API Complete, em uma página, sem grupos omitidos; 12 têm pontos e duas de logs-v2 não têm pontos (null, não zero). T1, T2, T3, T4 e coleta detalhada mantêm 7.417.226 bytes de estoque. Política STANDARD; nenhuma identidade diária StoredBytes foi recuperada. Delta líquido zero não prova geração zero.", "small")
    text("Lambda: 59.421 bytes e 198 eventos de log observados. O bucket 15:55-16:00 tem 4.096 bytes/20 eventos e está contido na janela; 15:50-15:55 tem 55.325/178 e sobrepõe o início, sem atribuição integral ao replay ou rateio pelo tempo. Há atividade histórica de prévia Glue no intervalo. A Lambda retém logs por 14 dias; seis grupos Glue não têm retenção explícita. Não houve alteração de retenção.", "small")
    heading("Enunciado e limites da conclusão")
    text("Página 4: justificar acesso, custo de leitura/transferência e atualização da fonte. Página 8: persistir volume, duração e status, medir latência e definir detecção de falhas. Página 9: tabela com serviço, unidade, quantidade, preço e projeção; estimar produção também em sandbox. A tabela da página 1 atende aos cinco campos, mas duas quantidades permanecem n.a.; não há garantia de custo total ou de aderência integral aos US$ 140.", "small")
    fee = state["measurement_reference_cost"]
    text("Medição pontual AWS: 14 métricas GetMetricData x US$ 0,0000100000 = US$ 0,0001400000 antes de franquias. SKU " + fee["sku"] + ", tarifa de 01/10/2026. Não se presumiu gratuidade; é preço de referência, não lançamento observado ou despesa recorrente. Não foi somado ao MTD nem subtraído dos créditos. Sem nova consulta Cost Explorer nesta etapa. Validação local: 17 testes dos coletores e analisador de logs; testes simulados não substituem a evidência da coleta real.", "small")
    heading("Fontes oficiais e arquivos")
    text("AWS IncomingBytes: " + METRICS_DOC + "\nAWS LogGroup.storedBytes: " + STOCK_DOC, "small")
    text("GCP preços: https://cloud.google.com/bigquery/pricing#data_extraction_pricing | Metadados: https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables\nSemântica das métricas API: https://docs.cloud.google.com/apis/docs/monitoring", "small")
    text("CloudWatch Pricing: https://aws.amazon.com/cloudwatch/pricing/ | Catálogo regional: " + fee["source_url"], "small")
    text("Pasta outputs/finops_pendencias_20261027: estado_pendencias.json; fechamento_logs.json; fechamento_gcp.json; raw/aws_logs/log_growth_readonly.json; raw/gcp_existing_metrics.json; modelo_finops.json. Hashes completos no QA. Novo modelo SHA-256: " + model["hashes"]["modelo_finops.json"], "small")
    text("Enunciado: " + str(model["paths"]["assignment_pdf"]) + "\nSHA-256: " + model["hashes"]["assignment_pdf"], "small")
    text("Para fechar: recuperar serializedResponseBytes auditados e correlacionados à extração, ou quantidade histórica de SKU de rede, se existirem. Reconciliar o arquivo comprimido após atividade existente com retenção e atribuição verificadas. Repetir response_sizes não estabelece equivalência de cobrança. Não iniciar nova extração apenas para obter números; manter n.a. se a fonte não tiver registrado os contadores.", "small")

    def footer(canvas, doc):
        canvas.saveState();canvas.setStrokeColor(line);canvas.line(margin, 31, page[0]-margin, 31)
        canvas.setFont("AddArial", 7.5);canvas.setFillColor(muted)
        canvas.drawString(margin, 20, "FIAP | Adendo FinOps | Medição somente de leitura | 2 parcelas continuam n.a.")
        canvas.drawRightString(page[0]-margin, 20, str(doc.page));canvas.restoreState()
    output.parent.mkdir(parents=True, exist_ok=True)
    doc=SimpleDocTemplate(str(output),pagesize=page,leftMargin=margin,rightMargin=margin,topMargin=28,bottomMargin=40,
                          title="FinOps - medições pendentes",author="Equipe Try Catch Finally")
    doc.build(story,onFirstPage=footer,onLaterPages=footer)
    pdf=PdfReader(output);assert len(pdf.pages)==3
    output.with_suffix(".txt").write_text("\n\n".join(txt)+"\n",encoding="utf-8")
    result={"status":"CREATED_PENDING_VISUAL_QA","pdf":str(output),"pages":3,"bytes":output.stat().st_size,
            "sha256":sha(output),"source_hashes":model["hashes"],"complete_total_usd":None,"unmeasured_cost_rows":2}
    output.with_suffix(".build.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    output.with_suffix(".sha256").write_text(sha(output)+"  "+output.name+"\n",encoding="utf-8")
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,default=BASE/"FinOps_Medicoes.pdf")
    args=parser.parse_args()
    assert not args.output.exists(), "Choose a new output filename; previous reports remain preserved"
    print(json.dumps(build(load(),args.output.resolve()),ensure_ascii=False))


if __name__=="__main__":main()
