"""Publish the reviewed Gold update on the existing job; optional single run.

No new resources, IAM changes, schedules or BigQuery reads are configured here.
Preserves existing job settings and records the previous configuration locally.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit
import uuid


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--directory", type=Path, default=Path(__file__).resolve().parent)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--region", default="us-east-2")
    p.add_argument("--expected-account", default="637246457028")
    p.add_argument("--job-name", default="fiap-gold-inep-batch")
    p.add_argument("--assets-bucket", default="aws-glue-assets-637246457028-us-east-2")
    p.add_argument("--expected-previous-sha256", default="3bc498f022924739bfd3a892a00a000604088a8b9c1b0439d07f1b1dcabca642")
    p.add_argument("--run", action="store_true", help="Start exactly one existing Gold job with the coverage Spark fixture")
    args = p.parse_args()
    if args.output_dir.exists():
        raise FileExistsError("Choose a new directory; preserve deployment evidence and avoid an accidental rerun")
    import boto3
    from botocore.exceptions import ClientError
    session = boto3.Session(region_name=args.region)
    if session.client("sts").get_caller_identity()["Account"] != args.expected_account:
        raise RuntimeError("Unexpected AWS account")
    s3, glue = session.client("s3"), session.client("glue")
    job = glue.get_job(JobName=args.job_name)["Job"]
    if (job.get("WorkerType"), job.get("NumberOfWorkers"), job.get("MaxRetries"), job.get("Timeout")) != ("G.1X", 2, 0, 15):
        raise RuntimeError("Job exceeds or differs from the reviewed 2 G.1X / zero retry / 15 minute configuration")
    active = [r for r in glue.get_job_runs(JobName=args.job_name, MaxResults=25)["JobRuns"]
              if r["JobRunState"] in {"STARTING", "RUNNING", "WAITING", "STOPPING"}]
    if active:
        raise RuntimeError("A Gold run is active; refusing to change the job")
    tags = glue.get_tags(ResourceArn=f"arn:aws:glue:{args.region}:{args.expected_account}:job/{args.job_name}")["Tags"]
    if tags.get("Project") != "FIAP-TC-Fase2" or tags.get("Component") != "gold-inep":
        raise RuntimeError("Expected project tags missing")
    script = args.directory / "gold_glue.py"
    notebook = args.directory / "03_camada_gold_glue.ipynb"
    content = script.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if b'SCRIPT_VERSION = "1.0.2"' not in content:
        raise RuntimeError("Expected the reviewed Gold v1.0.2")
    previous_uri = urlsplit(job["Command"]["ScriptLocation"])
    previous_bytes = s3.get_object(Bucket=previous_uri.netloc, Key=previous_uri.path.lstrip("/"))["Body"].read()
    previous_digest = hashlib.sha256(previous_bytes).hexdigest()
    if previous_digest not in {args.expected_previous_sha256, digest}:
        raise RuntimeError("Existing Gold code changed since review; refusing to overwrite")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    (args.output_dir / "job_before.json").write_text(json.dumps(job, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    (args.output_dir / "gold_before.py").write_bytes(previous_bytes)

    def upload(path, prefix):
        body = path.read_bytes()
        sha = hashlib.sha256(body).hexdigest()
        key = f"{prefix}/{args.job_name}/{sha}/{path.name}"
        try:
            s3.put_object(Bucket=args.assets_bucket, Key=key, Body=body, IfNoneMatch="*",
                          ServerSideEncryption="AES256", Metadata={"sha256": sha})
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
                raise
        readback = s3.get_object(Bucket=args.assets_bucket, Key=key)["Body"].read()
        if hashlib.sha256(readback).hexdigest() != sha:
            raise RuntimeError("S3 asset readback differs: " + key)
        return {"uri": f"s3://{args.assets_bucket}/{key}", "sha256": sha, "bytes": len(body)}

    assets = {"script": upload(script, "scripts"), "notebook": upload(notebook, "notebooks")}
    allowed = glue.meta.service_model.operation_model("UpdateJob").input_shape.members["JobUpdate"].members
    update = {key: deepcopy(value) for key, value in job.items() if key in allowed}
    # GetJob can expose legacy derived capacity alongside WorkerType. These
    # mutually exclusive input fields must not be sent together on UpdateJob.
    update.pop("AllocatedCapacity", None)
    update.pop("MaxCapacity", None)
    update["Command"]["ScriptLocation"] = assets["script"]["uri"]
    update.setdefault("DefaultArguments", {})["--VERIFY_COVERAGE_FIXTURE"] = "false"
    glue.update_job(JobName=args.job_name, JobUpdate=update)
    saved = glue.get_job(JobName=args.job_name)["Job"]
    for key in ("Command", "Role", "NumberOfWorkers", "WorkerType", "MaxRetries", "Timeout", "GlueVersion", "DefaultArguments"):
        if saved.get(key) != update.get(key):
            raise RuntimeError("Saved job setting differs: " + key)
    report = {"observed_at_utc": datetime.now(timezone.utc).isoformat(), "job_name": args.job_name,
              "account": args.expected_account, "region": args.region, "script_version": "1.0.2",
              "previous_script_uri": job["Command"]["ScriptLocation"], "previous_script_sha256": previous_digest,
              "assets": assets, "workers": saved["NumberOfWorkers"], "worker_type": saved["WorkerType"],
              "timeout_minutes": saved["Timeout"], "max_retries": saved["MaxRetries"],
              "silver_manifest_uri": saved.get("DefaultArguments", {}).get("--SILVER_MANIFEST_URI"),
              "new_resources": [], "iam_changes": False, "job_run_id": None,
              "run_start_requested": False, "deployment_id": "coverage_" + uuid.uuid4().hex}
    target = args.output_dir / "deployment.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.run:
        report["run_start_requested"] = True
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        # If the network outcome is unknown, do not retry this call blindly.
        # Find the run by its deployment id first; Glue StartJobRun is not idempotent.
        result = glue.start_job_run(JobName=args.job_name, Arguments={
            "--VERIFY_COVERAGE_FIXTURE": "true", "--DEMO_FAILURE": "false",
            "--COVERAGE_DEPLOYMENT_ID": report["deployment_id"]})
        report["job_run_id"] = result["JobRunId"]
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
