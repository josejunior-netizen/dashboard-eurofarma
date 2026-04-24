# Dashboard Hotéis Recorrentes — Eurofarma

Painel operacional para gestão de OS de hotelaria da conta Eurofarma.  
Atualização automática via GitHub Actions a cada **08h, 12h, 16h e 19h** (seg–sex, horário de Brasília).

---

## Estrutura do repositório

```
/
├── index.html              # Dashboard (HTML estático — GitHub Pages)
├── data.js                 # Dados gerados automaticamente (não editar)
├── scripts/
│   └── gerar_data.py       # Script Python que processa as bases e gera data.js
└── .github/workflows/
    └── atualizar-dashboard.yml   # GitHub Action agendada
```

---

## Configuração inicial (uma vez só)

### 1. Ativar GitHub Pages

No repositório → **Settings → Pages**  
Escolha: **Branch: main / pasta: / (root)**  
Salvar. O dashboard ficará disponível em `https://SEU-USUARIO.github.io/NOME-DO-REPO/`

### 2. Configurar os Secrets

No repositório → **Settings → Secrets and variables → Actions → New repository secret**

| Secret name            | Valor                                                      |
|------------------------|------------------------------------------------------------|
| `ONEDRIVE_DOWNLOAD_URL` | Link de download direto do `SourceHoteis_*.xls` no SharePoint |
| `SOURCING_DOWNLOAD_URL` | Link de download direto do `Sourcing_Finalizado_2026_Eurofarma.xlsx` no SharePoint |

> **Como obter o link de download direto do SharePoint:**  
> Abra o arquivo no SharePoint → `...` → Copiar link → Cole e substitua o final por `&download=1`  
> O link deve funcionar sem login (compartilhamento "Qualquer pessoa com o link").

### 3. Rodar a Action pela primeira vez

No repositório → **Actions → Atualizar Dashboard Eurofarma → Run workflow**  
Isso gera o `data.js` imediatamente sem esperar o agendamento.

---

## Horários de atualização

| Horário (BRT) | Cron (UTC)      |
|---------------|-----------------|
| 08:00         | `0 11 * * 1-5`  |
| 12:00         | `0 15 * * 1-5`  |
| 16:00         | `0 19 * * 1-5`  |
| 19:00         | `0 22 * * 1-5`  |

Para ajustar, edite `.github/workflows/atualizar-dashboard.yml`.

---

## Atualização do Sourcing

Quando o arquivo de Sourcing for atualizado:
1. Suba a nova versão para o mesmo local no SharePoint
2. Confirme que o link de download ainda funciona
3. A próxima execução da Action já usará os novos dados

---

## Dados preservados entre atualizações

O `localStorage` do browser salva automaticamente por OS:
- Status operacional
- Responsável da OS
- Observações/localizador
- Dono do hotel (responsável fixo)

Esses dados **não são apagados** quando o `data.js` é regenerado.

---

## Troubleshooting

| Problema | Causa provável | Solução |
|----------|---------------|---------|
| Dashboard em branco | `data.js` vazio ou Action não rodou | Rode a Action manualmente |
| Erro na Action | Link expirou ou sem permissão | Regere o link no SharePoint e atualize o Secret |
| Timestamp "Aguardando" | `data.js` é o placeholder | Rode a Action manualmente |
| CORS erro no fetch | Link não é público | Verifique permissão "Qualquer pessoa com o link" |
