# Usar imagem oficial Python
FROM python:3.11-slim

# Criar diretório
WORKDIR /app

# Copiar arquivos de dependência
COPY requirements.txt .

# Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo o projeto
COPY . .

# Expor porta padrão do Uvicorn
EXPOSE 8000

# Comando para rodar a API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
