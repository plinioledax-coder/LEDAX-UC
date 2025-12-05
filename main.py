from fastapi import FastAPI, Depends, Query, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from database import get_db, Base, engine 
from models import UnidadeComercial
from fastapi.staticfiles import StaticFiles
from typing import Optional, List 
from fastapi.responses import FileResponse

# ----------------------------------------
# CRIA O APP PRIMEIRO (ESSENCIAL)
# ----------------------------------------
app = FastAPI(title="MAPA UNIDADES API")

# Agora sim você pode montar o static
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", include_in_schema=False)
def serve_frontend():
    return FileResponse("static/index.html")

# ----------------------------------------------------
# 0. STARTUP
# ----------------------------------------------------
@app.on_event("startup")
def startup_event():
    Base.metadata.create_all(bind=engine)
    print("Banco de dados e tabelas verificados/criados com sucesso.")

# ----------------------------------------------------
# ROUTER
# ----------------------------------------------------
router = APIRouter(
    prefix="/unidades",
    tags=["unidades"],
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Test route
@app.get("/test")
def root_test():
    return {"status": "API is LIVE! Endpoints should be working."}

# Listar unidades
@router.get("/all")
def listar_unidades(db: Session = Depends(get_db)):
    return db.query(UnidadeComercial).filter(UnidadeComercial.latitude != None).all()

# Listar redes
@router.get("/redes")
def listar_redes(db: Session = Depends(get_db)):
    redes = db.query(UnidadeComercial.rede).distinct().order_by(UnidadeComercial.rede).all()
    return [r[0] for r in redes if r[0] is not None]

# Filtrar unidades
@router.get("/filtrar")
def filtrar(
    rede: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(UnidadeComercial).filter(UnidadeComercial.latitude != None)
    if rede:
        query = query.filter(UnidadeComercial.rede.in_(rede))
    return query.all()

# Inclui o router
app.include_router(router)
