"""Mantem os notebooks e os scripts .py das camadas com o mesmo codigo.

Cada notebook carrega UMA celula de codigo que e o script da camada sem o
rodape `if __name__ == "__main__"`. O script e a fonte da verdade: o notebook
serve para executar em sessao interativa do Glue e para guardar a narrativa e
as evidencias em markdown, que este utilitario nunca toca.

    python tools/sync_notebooks.py verificar    # falha (exit 1) se divergirem
    python tools/sync_notebooks.py atualizar    # copia script -> notebook

`verificar` roda tambem como teste automatizado em
`tests/test_sincronia_notebooks.py`, para que um ajuste feito so no script
nao chegue ao repositorio com o notebook desatualizado.
"""

from __future__ import annotations

import io
import json
import pathlib
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent
RODAPE = '\n\nif __name__ == "__main__":\n    main()\n'

# notebook -> (indice da celula de codigo, script correspondente)
PARES = {
    "bronze/camada_bronze_glue.ipynb": (3, "bronze/bronze_glue.py"),
    "silver/02_camada_silver_glue.ipynb": (3, "silver/silver_glue.py"),
    "gold/03_camada_gold_glue.ipynb": (4, "gold/gold_glue.py"),
}


def corpo_do_script(nome: str) -> str:
    """Le o script e devolve o codigo sem o rodape de execucao."""
    caminho = RAIZ / nome
    texto = pathlib.Path(caminho).read_text(encoding="utf-8")
    if not texto.endswith(RODAPE):
        raise SystemExit(f"{caminho}: o arquivo precisa terminar com o rodape __main__")
    # Linha em branco no fim da celula nao e diferenca de codigo.
    return texto[: -len(RODAPE)].rstrip("\n")


def celula(notebook: dict, indice: int, arquivo: str) -> dict:
    try:
        alvo = notebook["cells"][indice]
    except IndexError:
        raise SystemExit(f"{arquivo}: nao existe a celula {indice}")
    if alvo["cell_type"] != "code":
        raise SystemExit(f"{arquivo}: a celula {indice} nao e de codigo")
    return alvo


def verificar() -> int:
    divergentes = []
    for arquivo, (indice, script) in PARES.items():
        caminho = RAIZ / arquivo
        notebook = json.loads(pathlib.Path(caminho).read_text(encoding="utf-8"))
        atual = "".join(celula(notebook, indice, arquivo)["source"]).rstrip("\n")
        if atual != corpo_do_script(script):
            divergentes.append(f"{arquivo} != {script}")
    for linha in divergentes:
        print("DIVERGENTE:", linha)
    if divergentes:
        print("\nRode: python tools/sync_notebooks.py atualizar")
        return 1
    print(f"{len(PARES)} notebooks sincronizados com os scripts")
    return 0


def indentacao(texto: str) -> int:
    """Indentacao do JSON ja gravado, para a reescrita nao virar diff de arquivo inteiro."""
    for linha in texto.splitlines()[1:]:
        sem_recuo = linha.lstrip(" ")
        if sem_recuo:
            return len(linha) - len(sem_recuo)
    return 1


def atualizar() -> int:
    for arquivo, (indice, script) in PARES.items():
        caminho = RAIZ / arquivo
        bruto = pathlib.Path(caminho).read_text(encoding="utf-8")
        notebook = json.loads(bruto)
        alvo = celula(notebook, indice, arquivo)
        codigo = corpo_do_script(script)
        if "".join(alvo["source"]).rstrip("\n") == codigo:
            continue
        # Saidas de uma versao anterior do codigo deixariam de ser evidencia.
        alvo["source"] = codigo.splitlines(keepends=True)
        alvo["outputs"] = []
        alvo["execution_count"] = None
        with io.open(caminho, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(notebook, ensure_ascii=False,
                                indent=indentacao(bruto)) + "\n")
        print("atualizado:", arquivo)
    return 0


if __name__ == "__main__":
    comandos = {"verificar": verificar, "atualizar": atualizar}
    escolhido = sys.argv[1] if len(sys.argv) > 1 else ""
    if escolhido not in comandos:
        raise SystemExit(f"uso: python tools/sync_notebooks.py {' | '.join(comandos)}")
    raise SystemExit(comandos[escolhido]())
