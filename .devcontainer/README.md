# Codespaces da equipe

O `.devcontainer.json` define Python 3.13, uv 0.12.5, Node 24, Docker-in-Docker,
GitHub CLI e Google Cloud CLI. As dependências Python vêm do uv.lock; SQLFluff
fica isolado como ferramenta uv na versão 4.3.0.

Após atualizar a branch, execute **Codespaces: Rebuild Container** e conecte o
editor. O script postAttach instala e confere sete versões específicas de
extensões; falhas identificam a versão indisponível. As demais extensões do
projeto não são fixadas. Atualizações automáticas ficam desativadas por padrão.

O visual usa Sweet Dracula, perspectiva dbt Vintage, barra secundária oculta e
nenhuma dica inicial do terminal. Preferências pessoais podem sobrescrever esses
padrões. O alias Bash `dbtf` executa `uv run dbt` a partir do diretório atual.

Cada pessoa autentica suas próprias contas GitHub, Google Cloud e Altimate.
Nenhuma credencial ou configuração pessoal da VPS é copiada.

## Verificação após reconstruir

- Conferir `python --version`, `uv --version`, `node --version`, `gh --version`,
  `gcloud --version`, `sqlfluff --version` e `dbtf --version` em um terminal novo.
- Executar `docker info` para confirmar o daemon interno.
- Conferir `code --list-extensions --show-versions` contra extensions.sh.
- Abrir um SQL dbt, conferir o tema e a integração com o Python do projeto.
- Reconectar e abrir outro terminal: não deve haver aliases duplicados.

## OpenCode

O setup instala a CLI `opencode-ai@1.19.0` em `~/.local`, sem sudo, e verifica
a versão. Em um terminal novo, execute `opencode --version` ou `opencode`.
Cada pessoa autentica seus provedores; plugins, histórico e credenciais da VPS
não são copiados. A segunda execução preserva a instalação da versão correta.
