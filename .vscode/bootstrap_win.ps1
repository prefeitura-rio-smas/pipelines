# 🚀 rj-smas: One-Click Dev Setup (Windows)
# Esse script configura o ambiente de desenvolvimento SEM precisar de acesso administrador.

# Garante que o script rode na raiz do projeto, mesmo se chamado de dentro da .vscode
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location "$ScriptRoot\.."

Write-Host "--- Iniciando Setup rj-smas (Pasta: $((Get-Location).Path)) ---" -ForegroundColor Cyan

# 1. Instala o 'uv' (Gerenciador de Python ultrarrápido)
if (!(Get-Command "uv" -ErrorAction SilentlyContinue)) {
    Write-Host "-> Instalando uv (não requer admin)..."
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    $env:PATH += ";$HOME\.cargo\bin"
} else {
    Write-Host "-> uv já está instalado."
}

# 2. Instala Python 3.13 e as dependências do projeto
Write-Host "-> Criando ambiente virtual e instalando dbt (isso pode levar 1 min)..."
uv python install 3.13
uv sync

# 3. Orientações sobre o Google Cloud SDK (OAuth)
Write-Host "`n--- Autenticação Google Cloud ---" -ForegroundColor Yellow
if (!(Get-Command "gcloud" -ErrorAction SilentlyContinue)) {
    Write-Host "AVISO: gcloud CLI não encontrado no PATH."
    Write-Host "DICA: Se não tiver admin, baixe o 'Google Cloud SDK Archive' (.zip), extraia e adicione ao PATH."
} else {
    Write-Host "-> gcloud detectado. Para logar, execute no terminal do VS Code:"
    Write-Host "   gcloud auth application-default login" -ForegroundColor Green
}

Write-Host "`n--- Tudo pronto! ---" -ForegroundColor Cyan
Write-Host "1. Abra esta pasta no VS Code."
Write-Host "2. Pressione Ctrl+Shift+P e selecione 'Python: Select Interpreter'."
Write-Host "3. Escolha o interpretador que termina com '.venv\Scripts\python.exe'."
Write-Host "4. Abra um novo terminal e teste com: dbt debug"
