"""Confere os arquivos da Fase 2 contra SHA256_Arquivos.json.

O manifesto foi gerado na embalagem de 28/10/2026 e **nao** e regerado aqui: ele
registra o pacote como foi entregue. Este utilitario mostra o que mudou depois
disso, para que a divergencia seja auditavel em vez de silenciosa. As alteracoes
esperadas estao listadas em MIGRACAO_REPOSITORIO_20261028.md.

    python tools/verificar_integridade.py            # resumo
    python tools/verificar_integridade.py --detalhe  # lista cada arquivo divergente

Arquivo do manifesto que o .gitignore exclui de proposito nao conta como
ausente: num clone ele nao existe por politica, nao por perda. Sai com 1 apenas
se faltar arquivo que deveria estar versionado.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import sys

RAIZ = pathlib.Path(__file__).resolve().parent.parent
MANIFESTO = RAIZ / "SHA256_Arquivos.json"
# Fora do manifesto por serem anteriores ao pacote ou terem nascido na migracao.
IGNORADOS = {"SHA256_Arquivos.json", "Rascunho"}


def listar_arquivos() -> set[str]:
    encontrados = set()
    for caminho in RAIZ.rglob("*"):
        if not caminho.is_file():
            continue
        rel = caminho.relative_to(RAIZ).as_posix()
        if rel.split("/", 1)[0] in IGNORADOS or rel in IGNORADOS:
            continue
        encontrados.add(rel)
    return encontrados


def excluidos_literais(caminhos: list[str]) -> set[str]:
    """Plano B sem git (copia baixada como ZIP): so as regras sem curinga do
    .gitignore desta pasta, que e onde ficam as exclusoes por caminho exato."""
    regras = []
    for linha in (RAIZ / ".gitignore").read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if linha and not linha.startswith("#") and not any(c in linha for c in "*?[!"):
            regras.append(linha.lstrip("/"))
    return {c for c in caminhos
            if any(c == r or (r.endswith("/") and c.startswith(r)) for r in regras)}


def excluidos_pelo_gitignore(caminhos: list[str]) -> set[str]:
    """Quais caminhos o .gitignore exclui."""
    if not caminhos:
        return set()
    try:
        # -z e bytes: em modo texto o Windows troca \n por \r\n na entrada e
        # nenhum caminho casa com as regras.
        saida = subprocess.run(
            ["git", "check-ignore", "--no-index", "--stdin", "-z"], cwd=RAIZ,
            input="\0".join(caminhos).encode("utf-8"), capture_output=True, check=False)
    except FileNotFoundError:
        return excluidos_literais(caminhos)
    # exit 0 = algum ignorado, 1 = nenhum; outro codigo = fora de repositorio git
    if saida.returncode not in (0, 1):
        return excluidos_literais(caminhos)
    return {item for item in saida.stdout.decode("utf-8").split("\0") if item}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--detalhe", action="store_true")
    args = parser.parse_args(argv)

    manifesto = json.loads(MANIFESTO.read_text(encoding="utf-8"))
    excluidos = excluidos_pelo_gitignore(sorted(manifesto))
    ausentes, divergentes, fora_do_repo = [], [], []
    for rel, info in manifesto.items():
        if rel in excluidos:
            fora_do_repo.append(rel)
            continue
        caminho = RAIZ / rel
        if not caminho.is_file():
            ausentes.append(rel)
            continue
        if hashlib.sha256(caminho.read_bytes()).hexdigest() != info["sha256"]:
            divergentes.append((rel, info["bytes"], caminho.stat().st_size))
    novos = sorted(listar_arquivos() - set(manifesto) - excluidos_pelo_gitignore(
        sorted(listar_arquivos() - set(manifesto))))

    versionaveis = len(manifesto) - len(fora_do_repo)
    print(f"manifesto:            {len(manifesto)} arquivos")
    print(f"fora do repositorio:  {len(fora_do_repo)} (excluidos pelo .gitignore)")
    print(f"conferem:             {versionaveis - len(ausentes) - len(divergentes)}")
    print(f"ausentes:             {len(ausentes)}")
    print(f"alterados:            {len(divergentes)}")
    print(f"novos:                {len(novos)}")
    if args.detalhe or ausentes:
        for rel in fora_do_repo:
            print("  EXCLUIDO ", rel)
        for rel in ausentes:
            print("  AUSENTE  ", rel)
        for rel, antes, agora in divergentes:
            print(f"  ALTERADO  {rel} ({antes} -> {agora} bytes)")
        for rel in novos:
            print("  NOVO     ", rel)
    if ausentes:
        print("\nArquivo versionavel do manifesto sumiu: isso nao e esperado.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
