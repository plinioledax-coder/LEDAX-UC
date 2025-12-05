import os
import re
import time
import json
import pandas as pd
from sqlalchemy.orm import sessionmaker
from database import Base, engine, SessionLocal
from models import UnidadeComercial
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

# ==============================
# CONFIG
# ==============================
GEOCACHE_PATH = "data/geocache_uc.json"
EXCEL_PATH = "data/Tabela_UC.xlsx"
SAVE_CACHE_EVERY = 100
USER_AGENT = "ledax-mapa-unidades/2.0"

geolocator = Nominatim(user_agent=USER_AGENT, timeout=10)
geocode_limiter = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)

# ==============================
# CACHE
# ==============================
def load_cache(path=GEOCACHE_PATH):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            print("⚠️ Cache corrompido. Recriando.")
            return {}
    return {}

def save_cache(cache, path=GEOCACHE_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def cache_key(txt):
    if not txt:
        return None
    return txt.strip().upper()

GEOCACHE = load_cache()

# ==============================
# HELPERS — LIMPEZA DE ENDEREÇO
# ==============================
CEP_REGEX = re.compile(r"\b\d{5}-?\d{3}\b")

def extrair_cep(texto):
    m = CEP_REGEX.search(texto)
    return m.group(0) if m else None

def limpar_endereco(txt):
    if not isinstance(txt, str):
        return None
    txt = txt.strip()
    
    # Remove duplicações "----"
    txt = re.sub(r"-{2,}", "-", txt)

    # Remove múltiplos espaços
    txt = re.sub(r"\s+", " ", txt)

    # Corrige erros comuns de grafia
    txt = txt.replace("SALVADOR - BAHIA", "Salvador - BA")
    txt = txt.replace("SIMOES FILHO", "Simões Filho")
    txt = txt.replace("CAMACARI", "Camaçari")
    
    return txt

# ==============================
# GEOCODING
# ==============================
def geocode_try(q):
    """Busca geocode com cache."""
    if not q:
        return None, None

    key = cache_key(q)
    if key in GEOCACHE:
        return GEOCACHE[key].get("lat"), GEOCACHE[key].get("lon")

    try:
        loc = geocode_limiter(q + ", Brasil")
        if loc:
            GEOCACHE[key] = {"lat": loc.latitude, "lon": loc.longitude}
            return loc.latitude, loc.longitude
    except:
        pass

    GEOCACHE[key] = {"lat": None, "lon": None}
    return None, None


def geocode_inteligente(end):
    """
    Estratégia em cascata:
    1. Endereço limpo
    2. Endereço + UF (se faltar)
    3. CEP isolado
    4. Cidade detectada no texto
    5. Cidade + estado (fallback final)
    """

    if not end:
        return None, None, None

    end = limpar_endereco(end)
    original = end

    # 1 — endereço normal
    lat, lon = geocode_try(end)
    if lat: return lat, lon, original

    # 2 — se não tiver estado, tenta detectar
    if "-" in end:
        partes = end.split("-")
        if len(partes[-1].strip()) == 2:  # já tem UF
            pass
        else:
            end2 = f"{end} - BA"
            lat, lon = geocode_try(end2)
            if lat: return lat, lon, end2

    # 3 — procurar CEP
    cep = extrair_cep(end)
    if cep:
        lat, lon = geocode_try(cep)
        if lat: return lat, lon, cep

    # 4 — tentar extrair cidade de padrões conhecidos
    cidades_ba = ["Salvador", "Camaçari", "Lauro de Freitas", "Simões Filho", 
                  "Praia do Forte", "Acupe"]
    
    for c in cidades_ba:
        if c.lower() in end.lower():
            end3 = f"{end}, {c} - BA"
            lat, lon = geocode_try(end3)
            if lat: return lat, lon, end3

    # 5 — fallback final
    end4 = "Salvador - BA"
    lat, lon = geocode_try(end4)
    if lat: return lat, lon, end4

    return None, None, None


# ==============================
# ETL PRINCIPAL
# ==============================
def processar_excel(path_excel=EXCEL_PATH):
    print("📄 Lendo arquivo:", path_excel)

    try:
        df = pd.read_excel(path_excel)
    except:
        print("⚠️ Falha ao ler Excel.")
        return

    # Normaliza colunas
    df.columns = [re.sub(r"[^a-z0-9]+", "_", c.lower()) for c in df.columns]

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    total = len(df)

    print(f"\n🚀 Processando {total} registros...")
    
    for idx, row in tqdm(df.iterrows(), total=total):

        rede = row.get("rede")
        nome = row.get("nome")
        end = row.get("endere_o")
        cnpj = row.get("cnpj_cpf")

        if pd.isna(rede) or pd.isna(nome) or pd.isna(end):
            continue

        lat, lon, usado = geocode_inteligente(end)

        unidade = UnidadeComercial(
            rede=rede,
            nome=nome,
            endereco_original=end,
            cnpj=cnpj,
            endereco_usado_geocode=usado,
            latitude=lat,
            longitude=lon
        )

        db.add(unidade)

        if (idx + 1) % 200 == 0:
            db.commit()
        if (idx + 1) % SAVE_CACHE_EVERY == 0:
            save_cache(GEOCACHE)

    db.commit()
    db.close()
    save_cache(GEOCACHE)

    print("\n✅ ETL FINALIZADO!")
    print(f"📌 Cache total: {len(GEOCACHE)}")

# ==============================
if __name__ == "__main__":
    processar_excel(EXCEL_PATH)
