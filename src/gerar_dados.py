"""
Gerador de dados sintéticos para o case de engenharia de dados.

Gera três fontes simulando um ambiente de e-commerce real:
    - eventos: JSON Lines, particionado por dia, em MUITOS arquivos pequenos.
    - produtos: Parquet, particionado por uma chave inadequada.
    - usuarios: Parquet, arquivo único.

Uso:
    python gerar_dados.py --output ./dados_brutos
    python gerar_dados.py --output ./dados_brutos --escala pequena   # ~2M eventos (laptop)
    python gerar_dados.py --output ./dados_brutos --escala media     # ~20M eventos (default)
    python gerar_dados.py --output ./dados_brutos --escala grande    # ~50M eventos (cluster)

Dependências:
    pip install numpy pandas pyarrow
"""

import argparse
import gzip
import json
import os
import random
import shutil
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuração de escalas
# ---------------------------------------------------------------------------

ESCALAS = {
    "pequena": {"n_eventos": 2_000_000,  "n_usuarios": 50_000,  "n_produtos": 10_000},
    "media":   {"n_eventos": 20_000_000, "n_usuarios": 500_000, "n_produtos": 50_000},
    "grande":  {"n_eventos": 50_000_000, "n_usuarios": 1_000_000, "n_produtos": 80_000},
}

DIAS_HISTORICO = 30
DATA_FIM = datetime(2025, 3, 31)
SEED = 42

CATEGORIAS = {
    "eletronicos": ["smartphones", "notebooks", "tvs", "audio", "acessorios"],
    "moda":        ["camisetas", "calcas", "tenis", "vestidos", "acessorios"],
    "casa":        ["moveis", "decoracao", "cozinha", "cama_mesa_banho"],
    "esporte":     ["futebol", "corrida", "musculacao", "ciclismo"],
    "beleza":      ["maquiagem", "perfumes", "cabelo", "skincare"],
    "livros":      ["ficcao", "tecnico", "infantil", "autoajuda"],
}

MARCAS = [f"marca_{i:03d}" for i in range(200)]
PAISES = ["BR", "BR", "BR", "BR", "AR", "MX", "CO", "CL", "PE", "US"]  # BR dominante
DEVICES = ["mobile", "mobile", "mobile", "desktop", "desktop", "tablet"]
EVENT_TYPES = ["view", "view", "view", "view", "view", "view", "view",
               "add_to_cart", "add_to_cart", "purchase"]
AGE_BANDS = ["18-24", "25-34", "35-44", "45-54", "55+"]

UTM_SOURCES = ["google", "facebook", "instagram", "tiktok", "email", "direct", None]
UTM_CAMPAIGNS = [f"camp_{i:03d}" for i in range(50)] + [None] * 20


# ---------------------------------------------------------------------------
# Geração de produtos
# ---------------------------------------------------------------------------

def gerar_produtos(n_produtos: int, output_dir: Path) -> pd.DataFrame:
    print(f"[produtos] gerando {n_produtos:,} produtos...")
    rng = np.random.default_rng(SEED)

    rows = []
    categorias_lista = list(CATEGORIAS.keys())
    for pid in range(1, n_produtos + 1):
        cat = rng.choice(categorias_lista)
        sub = rng.choice(CATEGORIAS[cat])
        marca = rng.choice(MARCAS)
        preco = float(np.round(rng.lognormal(mean=4.5, sigma=0.8), 2))
        rows.append({
            "product_id": pid,
            "category": cat,
            "subcategory": sub,
            "brand": marca,
            "price": preco,
        })

    df = pd.DataFrame(rows)

    # PEGADINHA: particionamento ruim — por subcategoria (média cardinalidade)
    # com arquivos minúsculos por partição. Estudante deve repensar isso.
    out = output_dir / "produtos"
    if out.exists():
        shutil.rmtree(out)
    df.to_parquet(out, partition_cols=["subcategory"], index=False)

    print(f"[produtos] salvo em {out} (particionado por subcategory)")
    return df


# ---------------------------------------------------------------------------
# Geração de usuários
# ---------------------------------------------------------------------------

def gerar_usuarios(n_usuarios: int, output_dir: Path) -> pd.DataFrame:
    print(f"[usuarios] gerando {n_usuarios:,} usuários...")
    rng = np.random.default_rng(SEED + 1)

    user_ids = np.arange(1, n_usuarios + 1)
    countries = rng.choice(PAISES, size=n_usuarios)
    age_bands = rng.choice(AGE_BANDS, size=n_usuarios, p=[0.15, 0.35, 0.25, 0.15, 0.10])

    base_date = datetime(2020, 1, 1)
    days_since = rng.integers(0, (DATA_FIM - base_date).days, size=n_usuarios)
    signup_dates = [base_date + timedelta(days=int(d)) for d in days_since]

    df = pd.DataFrame({
        "user_id": user_ids,
        "country": countries,
        "signup_date": signup_dates,
        "age_band": age_bands,
    })

    out = output_dir / "usuarios"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "usuarios.parquet", index=False)

    print(f"[usuarios] salvo em {out}")
    return df


# ---------------------------------------------------------------------------
# Geração de eventos (JSON Lines particionado por dia, com small files)
# ---------------------------------------------------------------------------

def _gerar_distribuicao_skewed(n_produtos: int, rng: np.random.Generator) -> np.ndarray:
    """
    Distribuição Zipf truncada: poucos produtos concentram muito tráfego.
    Top 1% dos produtos recebe ~50% dos eventos.
    """
    pesos = 1.0 / np.power(np.arange(1, n_produtos + 1), 1.2)
    pesos = pesos / pesos.sum()
    # Embaralha pra que os "produtos quentes" não sejam os de id baixo
    perm = rng.permutation(n_produtos)
    pesos_embaralhados = np.empty_like(pesos)
    pesos_embaralhados[perm] = pesos
    return pesos_embaralhados


def _serializar_evento(evento: dict) -> str:
    """Serializa um evento como JSON. Alguns campos podem estar ausentes."""
    return json.dumps(evento, ensure_ascii=False, default=str)


def gerar_eventos(
    n_eventos: int,
    n_usuarios: int,
    n_produtos: int,
    output_dir: Path,
) -> None:
    print(f"[eventos] gerando {n_eventos:,} eventos em {DIAS_HISTORICO} dias...")
    rng = np.random.default_rng(SEED + 2)

    out = output_dir / "eventos"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    # Distribuição enviesada de produtos (skew controlado)
    pesos_produtos = _gerar_distribuicao_skewed(n_produtos, rng)

    # Distribuição de usuários: alguns power users
    pesos_usuarios = 1.0 / np.power(np.arange(1, n_usuarios + 1), 0.5)
    pesos_usuarios = pesos_usuarios / pesos_usuarios.sum()
    perm_u = rng.permutation(n_usuarios)
    pesos_usuarios_emb = np.empty_like(pesos_usuarios)
    pesos_usuarios_emb[perm_u] = pesos_usuarios

    eventos_por_dia = n_eventos // DIAS_HISTORICO
    data_inicio = DATA_FIM - timedelta(days=DIAS_HISTORICO - 1)

    for dia_idx in range(DIAS_HISTORICO):
        dia = data_inicio + timedelta(days=dia_idx)
        pasta_dia = out / f"dt={dia.strftime('%Y-%m-%d')}"
        pasta_dia.mkdir(parents=True, exist_ok=True)

        # PEGADINHA 1 (small files): particionar cada dia em ~120 arquivinhos.
        # Simula ingestão por micro-batches (ex: stream a cada ~12 min).
        n_arquivos_dia = 120
        eventos_por_arquivo = eventos_por_dia // n_arquivos_dia

        # Amostragens em lote pra performance
        product_ids = rng.choice(
            np.arange(1, n_produtos + 1),
            size=eventos_por_dia,
            p=pesos_produtos,
        )
        user_ids = rng.choice(
            np.arange(1, n_usuarios + 1),
            size=eventos_por_dia,
            p=pesos_usuarios_emb,
        )
        event_types = rng.choice(EVENT_TYPES, size=eventos_por_dia)
        devices = rng.choice(DEVICES, size=eventos_por_dia)

        # PEGADINHA 2 (sujeira de dados):
        # ~0.5% de product_id órfão (não existe no catálogo)
        mask_orfao_prod = rng.random(eventos_por_dia) < 0.005
        product_ids[mask_orfao_prod] = rng.integers(
            n_produtos + 1_000_000, n_produtos + 2_000_000, size=mask_orfao_prod.sum()
        )

        # ~0.3% de user_id órfão
        mask_orfao_user = rng.random(eventos_por_dia) < 0.003
        user_ids[mask_orfao_user] = rng.integers(
            n_usuarios + 1_000_000, n_usuarios + 2_000_000, size=mask_orfao_user.sum()
        )

        # Timestamps espalhados ao longo do dia
        segundos_no_dia = rng.integers(0, 86400, size=eventos_por_dia)

        # Sessões: agrupa usuários em sessões curtas
        session_ids = [
            f"sess_{uuid.UUID(int=int(u) * 1_000_000 + int(s // 1800)).hex[:16]}"
            for u, s in zip(user_ids, segundos_no_dia)
        ]

        # PEGADINHA 3 (semi-estruturado):
        # Schema com campos opcionais e aninhados que precisam ser achatados.
        for arquivo_idx in range(n_arquivos_dia):
            ini = arquivo_idx * eventos_por_arquivo
            fim = ini + eventos_por_arquivo if arquivo_idx < n_arquivos_dia - 1 else eventos_por_dia

            linhas = []
            for i in range(ini, fim):
                ts = dia + timedelta(seconds=int(segundos_no_dia[i]))
                evento = {
                    "event_id": uuid.uuid4().hex,
                    "event_timestamp": ts.isoformat(),
                    "event_type": str(event_types[i]),
                    "user_id": int(user_ids[i]),
                    "product_id": int(product_ids[i]),
                    "session_id": session_ids[i],
                    # Aninhado: contexto do dispositivo
                    "context": {
                        "device": str(devices[i]),
                        "user_agent": _fake_user_agent(rng),
                        "ip_hash": _fake_ip_hash(rng),
                    },
                    # Aninhado opcional: marketing (pode estar ausente)
                    "marketing": None if rng.random() < 0.4 else {
                        "utm_source": _safe_choice(UTM_SOURCES, rng),
                        "utm_campaign": _safe_choice(UTM_CAMPAIGNS, rng),
                    },
                }

                # PEGADINHA 4 (schema evolution):
                # Campo `quantity` só existe pra event_type == 'add_to_cart' ou 'purchase'.
                if evento["event_type"] in ("add_to_cart", "purchase"):
                    evento["quantity"] = int(rng.integers(1, 5))

                # PEGADINHA 5 (tipo inconsistente):
                # Em ~1% dos casos, price_paid vem como string (erro upstream).
                if evento["event_type"] == "purchase":
                    valor = float(np.round(rng.lognormal(4.5, 0.8), 2))
                    evento["price_paid"] = str(valor) if rng.random() < 0.01 else valor

                linhas.append(_serializar_evento(evento))

            arquivo = pasta_dia / f"events_{arquivo_idx:04d}.jsonl"
            with open(arquivo, "w", encoding="utf-8") as f:
                f.write("\n".join(linhas))

        if (dia_idx + 1) % 5 == 0 or dia_idx == DIAS_HISTORICO - 1:
            print(f"[eventos]   {dia_idx + 1}/{DIAS_HISTORICO} dias gerados")

    print(f"[eventos] salvo em {out} ({DIAS_HISTORICO} partições, ~{n_arquivos_dia} arquivos/dia)")


# ---------------------------------------------------------------------------
# Auxiliares
# ---------------------------------------------------------------------------

def _safe_choice(lista, rng):
    return lista[rng.integers(0, len(lista))]

def _fake_user_agent(rng):
    base = ["Mozilla/5.0", "AppleWebKit/537.36", "Chrome/120.0.0.0", "Safari/537.36"]
    return " ".join(base) + f" build/{rng.integers(1000, 9999)}"

def _fake_ip_hash(rng):
    return "".join(rng.choice(list(string.hexdigits.lower()), size=16))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="./dados_brutos", help="diretório de saída")
    parser.add_argument("--escala", default="media", choices=list(ESCALAS.keys()))
    args = parser.parse_args()

    random.seed(SEED)
    np.random.seed(SEED)

    config = ESCALAS[args.escala]
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"=== Geração de dados — escala '{args.escala}' ===")
    print(f"  eventos:  {config['n_eventos']:,}")
    print(f"  usuários: {config['n_usuarios']:,}")
    print(f"  produtos: {config['n_produtos']:,}")
    print(f"  saída:    {output_dir.resolve()}")
    print()

    gerar_produtos(config["n_produtos"], output_dir)
    gerar_usuarios(config["n_usuarios"], output_dir)
    gerar_eventos(
        n_eventos=config["n_eventos"],
        n_usuarios=config["n_usuarios"],
        n_produtos=config["n_produtos"],
        output_dir=output_dir,
    )

    print("\n=== concluído ===")


if __name__ == "__main__":
    main()
