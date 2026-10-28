"""Sync the adapted notebook's code cell with gold_glue.py. Local files only."""
import argparse
import ast
import json
from pathlib import Path


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--directory", default="gold")
    args = p.parse_args(argv)
    directory = Path(args.directory)
    script = (directory / "gold_glue.py").read_text(encoding="utf-8")
    ast.parse(script)
    marker = 'if __name__ == "__main__":'
    if script.count(marker) != 1:
        raise ValueError("Expected exactly one batch entry-point guard")
    body = script.rsplit(marker, 1)[0].rstrip() + "\n"
    ast.parse(body)
    path = directory / "03_camada_gold_glue.ipynb"
    notebook = json.loads(path.read_text(encoding="utf-8"))
    cells = [c for c in notebook["cells"] if c.get("cell_type") == "code"
             and "def construir_validacao_cruzada_brasil" in "".join(c.get("source", []))]
    if len(cells) != 1:
        raise ValueError("Expected exactly one Gold implementation cell")
    cells[0]["source"] = body.splitlines(keepends=True)
    for cell in notebook["cells"]:
        if cell.get("cell_type") == "code":
            cell["execution_count"], cell["outputs"] = None, []
    path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print("Notebook adaptado sincronizado:", path.resolve())
    print("Notebook original preservado; nenhuma chamada AWS.")


if __name__ == "__main__":
    main()
