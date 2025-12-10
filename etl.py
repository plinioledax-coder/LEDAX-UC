import os
import re
import json
import time
import requests
import pandas as pd
from sqlalchemy.orm import sessionmaker
from database import Base, engine, SessionLocal
from models import UnidadeComercial
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.distance import geodesic # <--- IMPORTANTE PARA CALCULAR DISTANCIA

# ==============================
# CONFIGURAÇÕES
# ==============================
GEOCACHE_PATH = "data/geocache_uc.json"
EXCEL_PATH = "data/Tabela_UC.xlsx"
USER_AGENT = "ledax-mapa-interactive/7.0-geofence"

# Configuração da CERCA VIRTUAL (Salvador + RMS)
# Se cair longe disso, é erro.
SALVADOR_CENTROID = (-12.9714, -38.5014) 
RAIO_MAXIMO_KM = 150  # 150km cobre Salvador, Camaçari, Feira, Lauro com folga.

geolocator = Nominatim(user_agent=USER_AGENT, timeout=10)
geocode_limiter = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)

# ==============================
# CACHE
# ==============================
def load_cache():
    if os.path.exists(GEOCACHE_PATH):
        try:
            with open(GEOCACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except: return {}
    return {}

def save_cache(cache):
    os.makedirs(os.path.dirname(GEOCACHE_PATH), exist_ok=True)
    with open(GEOCACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def cache_key(data):
    if isinstance(data, dict): return json.dumps(data, sort_keys=True).upper()
    return str(data).strip().upper()

GEOCACHE = load_cache()

# ==============================
# HELPERS
# ==============================
def tratar_cep_excel(valor):
    """Limpa e formata o CEP da coluna"""
    if pd.isna(valor) or valor == "": return None
    s = str(valor).split('.')[0] # Remove decimais
    s_numeros = re.sub(r'\D', '', s)
    
    if not s_numeros: return None
    if len(s_numeros) < 8: s_numeros = s_numeros.zfill(8)
    if len(s_numeros) > 8: s_numeros = s_numeros[:8]

    return f"{s_numeros[:5]}-{s_numeros[5:]}"

def limpar_endereco(txt):
    if not isinstance(txt, str): return ""
    txt = txt.upper().strip()
    termos = [r"\bLOJA\b", r"\bLJ\b", r"\bT[ÉE]RREO\b", r"\bSALA\b", r"\bANDAR\b", r"\bBOX\b", r"\bREF:?"]
    for t in termos:
        m = re.search(t, txt)
        if m: txt = txt[:m.start()]
    txt = re.sub(r"[^\w\s,\-]", "", txt)
    return re.sub(r"\s+", " ", txt).strip()

def extrair_cidade(texto):
    if not isinstance(texto, str): return "Salvador"
    texto = texto.upper()
    cidades = ["CAMAÇARI", "CAMACARI", "LAURO DE FREITAS", "SIMÕES FILHO", "SIMOES FILHO", "DIAS D'AVILA", "MATA DE SAO JOAO"]
    for cid in cidades:
        if cid in texto:
            return cid.replace("CAMACARI", "CAMAÇARI").replace("SIMOES", "SIMÕES").title()
    return "Salvador"

# --- NOVO: TRAVA DE SEGURANÇA GEOGRÁFICA ---
def is_fora_da_area(lat, lon):
    """
    Retorna TRUE se o ponto estiver perigosamente longe de Salvador (outros estados/interior).
    """
    if not lat or not lon: return True
    try:
        dist = geodesic((lat, lon), SALVADOR_CENTROID).km
        return dist > RAIO_MAXIMO_KM
    except:
        return True

# ==============================
# BUSCA API
# ==============================
def buscar_coordenadas(query_input):
    """Executa a busca (BrasilAPI ou Nominatim)"""
    
    # 1. BRASIL API (Prioridade Total para CEP)
    if isinstance(query_input, str) and re.match(r"^\d{5}-?\d{3}$", query_input.strip()):
        cep_limpo = query_input.replace("-", "")
        try:
            r = requests.get(f"https://brasilapi.com.br/api/cep/v1/{cep_limpo}", timeout=2)
            if r.status_code == 200:
                d = r.json()
                # Correção segura para V1/V2
                loc = d.get("location", {}).get("coordinates", {})
                lat = loc.get("latitude")
                lon = loc.get("longitude")
                
                # Fallback structure check
                if not lat and "location" in d and "coordinates" in d["location"]:
                     lat = d["location"]["coordinates"].get("latitude")
                     lon = d["location"]["coordinates"].get("longitude")

                if lat:
                    end_fmt = f"{d.get('street', '')}, {d.get('neighborhood', '')}, {d.get('city', '')} - {d.get('state','')}"
                    return float(lat), float(lon), f"BrasilAPI ({query_input}): {end_fmt}"
        except: pass

    # 2. NOMINATIM
    key = cache_key(query_input)
    if key in GEOCACHE:
        c = GEOCACHE[key]
        if c.get("lat"): return c["lat"], c["lon"], c["display_name"]
        return None, None, None

    try:
        loc = geocode_limiter(query=query_input, addressdetails=True, country_codes="br")
        if loc:
            GEOCACHE[key] = {"lat": loc.latitude, "lon": loc.longitude, "display_name": loc.address}
            return loc.latitude, loc.longitude, loc.address
    except Exception as e:
        print(f"Erro API: {e}")
    
    GEOCACHE[key] = {"lat": None, "lon": None}
    return None, None, None

def tentar_automacao(end_orig, cidade_orig, cep_prioritario=None):
    
    end_limpo = limpar_endereco(end_orig)
    
    # Prepara nome da rua (remove números)
    rua_full = end_limpo.split(',')[0].strip()
    rua_sem_num = re.sub(r'\d+', '', rua_full).strip()
    
    log_msg = ""
    if cep_prioritario: log_msg = f"[CEP Coluna: {cep_prioritario}]"
    
    # --- 1: BrasilAPI com CEP da Coluna (OURO) ---
    if cep_prioritario:
        lat, lon, src = buscar_coordenadas(cep_prioritario)
        if lat: return lat, lon, f"{src} {log_msg}"

    # --- 2: Nominatim (Rua + Cidade + CEP Coluna) ---
    if rua_sem_num:
        q = {"street": rua_sem_num, "city": cidade_orig, "state": "BA", "country": "Brazil"}
        if cep_prioritario: q["postalcode"] = cep_prioritario
        lat, lon, src = buscar_coordenadas(q)
        if lat: return lat, lon, f"{src} {log_msg}"

    # --- 3: Fallback (Rua + BA) ---
    if rua_sem_num:
        q_glob = {"street": rua_sem_num, "state": "BA", "country": "Brazil"}
        lat, lon, src = buscar_coordenadas(q_glob)
        if lat: return lat, lon, f"{src} {log_msg}"

    return None, None, None

# ==============================
# INTERFACE MANUAL
# ==============================
def resolver_manual(nome_loja, end_excel, cidade_excel, candidato_auto, cep_coluna, motivo_erro=""):
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n" + "!"*60)
    print(f"🛑 INTERVENÇÃO NECESSÁRIA: {motivo_erro}")
    print("!"*60)
    print(f"🏢 LOJA:   {nome_loja}")
    print(f"📄 END:    {end_excel}")
    print(f"📮 CEP COLUNA: {cep_coluna if cep_coluna else 'N/A'}")
    print("-" * 60)

    lat_cand, lon_cand, end_cand = candidato_auto

    if lat_cand:
        print(f"🤖 SUGERIDO: {end_cand}")
        print(f"🌍 Coords:   {lat_cand:.5f}, {lon_cand:.5f}")
        
        # Calcula distancia para mostrar ao usuario
        dist = geodesic((lat_cand, lon_cand), SALVADOR_CENTROID).km
        print(f"📏 Distância do Centro de Salvador: {dist:.1f} km")
        
        if dist > RAIO_MAXIMO_KM:
            print("⚠️  AVISO CRÍTICO: PONTO MUITO DISTANTE (OUTRO ESTADO?)")
    else:
        print(f"❌ Nenhuma localização automática confiável encontrada.")

    print("-" * 60)
    print("[A]ceitar (Apenas se tiver certeza) | [M]anual | [G]oogle Maps | [P]ular")

    while True:
        choice = input("👉 Opção: ").lower().strip()

        if choice == 'a' and lat_cand:
            return lat_cand, lon_cand, f"Aprovado Manualmente (Alerta): {end_cand}"
        
        if choice == 'p':
            return None, None, "Pulado"
            
        if choice == 'g':
            q_link = f"{nome_loja} {end_excel}".replace(" ", "+")
            print(f"🔗 https://www.google.com/maps/search/{q_link}")
            continue

        if choice == 'm':
            print("\n💡 Dica: Digite APENAS O CEP correto ou 'Rua X, Cidade'")
            nova = input("🔎 Digite Busca: ")
            l, lo, e = buscar_coordenadas(nova)
            if l:
                d_nova = geodesic((l, lo), SALVADOR_CENTROID).km
                print(f"✅ Achou: {e}")
                print(f"📏 Distância: {d_nova:.1f} km")
                
                if d_nova > RAIO_MAXIMO_KM:
                    print("⚠️  CUIDADO: Isso ainda está muito longe de Salvador!")
                
                if input("   Usar este? (s/n): ") == 's':
                    return l, lo, f"Manual: {e}"
            else:
                print("❌ Nada encontrado.")

# ==============================
# MAIN
# ==============================
def processar_excel():
    print("📄 Lendo Excel...")
    try:
        df = pd.read_excel(EXCEL_PATH)
    except Exception as e:
        print(e); return

    df.columns = [re.sub(r"[^a-z0-9]+", "_", c.lower()) for c in df.columns]

    # Identifica coluna de CEP
    col_cep = None
    for c in df.columns:
        if ('cep' in c or 'postal' in c) and 'recep' not in c:
            col_cep = c
            break
    
    if col_cep: print(f"🎯 Usando Coluna de CEP: '{col_cep}'")
    else: print("⚠️ Nenhuma coluna 'CEP' encontrada.")

    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    total = len(df)
    
    print(f"\n🚀 Iniciando com CERCA VIRTUAL (Raio {RAIO_MAXIMO_KM}km de Salvador)...\n")
    time.sleep(2)

    for idx, row in df.iterrows():
        rede = row.get("rede")
        nome = row.get("nome")
        end = row.get("endere_o")
        
        cep_da_coluna = None
        if col_cep:
            cep_da_coluna = tratar_cep_excel(row.get(col_cep))

        if pd.isna(end): continue

        print(f"[{idx+1}/{total}] {nome[:30]}...", end="\r")

        cidade_excel = extrair_cidade(end)
        
        # 1. Tenta Automático
        lat, lon, src = tentar_automacao(end, cidade_excel, cep_prioritario=cep_da_coluna)
        
        # 2. VALIDAÇÃO RIGOROSA
        motivo = ""
        intervir = False
        
        if not lat:
            intervir = True
            motivo = "Não encontrado"
        
        # CHECK 1: Está fora da RMS?
        elif is_fora_da_area(lat, lon):
            intervir = True
            motivo = "FORA DA ÁREA (Salvador/RMS)"
            
        # CHECK 2: Cidade diverge? (Opcional, mas bom manter)
        elif "camaçari" in cidade_excel.lower() and "salvador" in src.lower() and "Nominatim" in src:
            # As vezes o Nominatim joga Camaçari no centro de Salvador se errar a rua
            # Mas a checagem de distancia acima ja pega os casos grossos (Manaus/Paraná)
            pass 

        metodo_final = src
        if intervir:
            lat, lon, metodo_final = resolver_manual(
                nome, end, cidade_excel, (lat, lon, src), cep_da_coluna, motivo
            )
        
        if lat and lon:
            unidade = UnidadeComercial(
                rede=rede, nome=nome, endereco_original=end, cnpj=row.get("cnpj_cpf"),
                endereco_usado_geocode=metodo_final, latitude=lat, longitude=lon
            )
            db.add(unidade)
            db.commit()
            save_cache(GEOCACHE)
            print(f"✅ Salvo: {nome}")
        else:
            print(f"⏭️  Pulado: {nome}")

    print("\n🏁 FIM!")

if __name__ == "__main__":
    processar_excel()