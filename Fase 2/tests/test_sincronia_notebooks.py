"""Garante que notebook e script da mesma camada carregam o codigo identico.

O script e a fonte da verdade; o notebook existe para rodar em sessao
interativa do Glue e para guardar a narrativa e as evidencias. Sem este teste,
um ajuste feito so no script chega ao repositorio com o notebook mostrando
codigo antigo — e o avaliador le o notebook.

    python -m unittest discover -s "Fase 2/tests" -v
"""

from __future__ import annotations

import json
import pathlib
import unittest

RAIZ = pathlib.Path(__file__).resolve().parent.parent
RODAPE = '\n\nif __name__ == "__main__":\n    main()\n'
PARES = {
    "bronze/camada_bronze_glue.ipynb": (3, "bronze/bronze_glue.py"),
    "silver/02_camada_silver_glue.ipynb": (3, "silver/silver_glue.py"),
    "gold/03_camada_gold_glue.ipynb": (4, "gold/gold_glue.py"),
}


class TestSincroniaNotebooks(unittest.TestCase):
    def test_codigo_do_notebook_e_o_do_script(self):
        for arquivo, (indice, script) in PARES.items():
            with self.subTest(notebook=arquivo):
                caminho_script = RAIZ / script
                codigo = pathlib.Path(caminho_script).read_text(encoding="utf-8")
                self.assertTrue(codigo.endswith(RODAPE),
                                f"{script} precisa terminar com o rodape __main__")
                notebook = json.loads(pathlib.Path(RAIZ / arquivo).read_text(encoding="utf-8"))
                celula = notebook["cells"][indice]
                self.assertEqual(celula["cell_type"], "code")
                self.assertEqual("".join(celula["source"]).rstrip("\n"),
                                 codigo[: -len(RODAPE)].rstrip("\n"),
                                 f"{arquivo} divergiu de {script}; "
                                 "rode python tools/sync_notebooks.py atualizar")

    def test_todo_script_de_camada_esta_coberto(self):
        esperados = {RAIZ / script for _, script in PARES.values()}
        encontrados = {caminho for caminho in RAIZ.glob("*/*_glue.py")}
        self.assertEqual(encontrados, esperados,
                         "ha script de camada sem notebook pareado nesta lista")


if __name__ == "__main__":
    unittest.main()
