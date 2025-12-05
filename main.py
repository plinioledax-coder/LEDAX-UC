from fastapi import FastAPI, Depends, Query, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
# Importa tudo que é necessário do database.py (get_db, Base e engine)
from database import get_db, Base, engine 
from models import UnidadeComercial # Garante que o modelo UnidadeComercial seja importado
from fastapi.staticfiles import StaticFiles
from typing import Optional, List 

app = FastAPI(title="MAPA UNIDADES API")

# ----------------------------------------------------
# 0. FUNÇÃO DE STARTUP PARA CRIAR AS TABELAS (CRÍTICO)
# ----------------------------------------------------
@app.on_event("startup")
def startup_event():
    # Cria todas as tabelas no banco de dados, se ainda não existirem.
    Base.metadata.create_all(bind=engine)
    print("Banco de dados e tabelas verificados/criados com sucesso.")


# ----------------------------------------------------
# 1. SETUP DO APIRouter para todas as rotas /unidades
# ----------------------------------------------------
router = APIRouter(
    prefix="/unidades", # Define o prefixo '/unidades' para todas as rotas abaixo
    tags=["unidades"],
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# ROTA DE TESTE CRÍTICA (Permanecendo na raiz do APP)
# -------------------------------
@app.get("/test")
def root_test():
    return {"status": "API is LIVE! Endpoints should be working."}

# 🚨 LINHA REMOVIDA TEMPORARIAMENTE PARA TESTAR CONFLITO:
# app.mount("/", StaticFiles(directory="static", html=True), name="static")


# -------------------------------
# ENDPOINT 1 - Lista Unidades (Rota final: /unidades/all)
# -------------------------------
@router.get("/all")
def listar_unidades(db: Session = Depends(get_db)):
    """Retorna todas as unidades com coordenadas válidas."""
    return db.query(UnidadeComercial).filter(UnidadeComercial.latitude != None).all()


# -------------------------------
# ENDPOINT 2 - Lista de Redes (Rota final: /unidades/redes)
# -------------------------------
@router.get("/redes")
def listar_redes(db: Session = Depends(get_db)):
    """Retorna uma lista de redes únicas para popular o filtro do front-end."""
    redes = db.query(UnidadeComercial.rede).distinct().order_by(UnidadeComercial.rede).all()
    return [r[0] for r in redes if r[0] is not None]

# -------------------------------
# ENDPOINT 3 - Filtragem (por rede) (Rota final: /unidades/filtrar)
# -------------------------------
@router.get("/filtrar")
def filtrar(
    rede: Optional[List[str]] = Query(None, description="Lista de redes a serem filtradas"), 
    db: Session = Depends(get_db)
):
    """Filtra unidades com base nas redes selecionadas."""
    query = db.query(UnidadeComercial).filter(UnidadeComercial.latitude != None) 
    
    if rede:
        query = query.filter(UnidadeComercial.rede.in_(rede))
        
    return query.all()

# ----------------------------------------------------
# 2. INCLUI O ROUTER NO APP PRINCIPAL
# ----------------------------------------------------
app.include_router(router)