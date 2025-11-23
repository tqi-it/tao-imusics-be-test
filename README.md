# 📊 tao-imusics-be-test : Projeto de Validação de Sumarização – iMusics Analytics

Este repositório contém um conjunto de **testes automatizados** desenvolvidos para validar todo o fluxo de **processamento**, **armazenamento**, **sumarização** e **consistência de dados** do pipeline de Analytics utilizado pela plataforma TAO / iMusics.

O foco deste projeto é garantir a integridade dos dados desde sua origem (arquivos TSV) até a geração das métricas sumarizadas consumidas pelo frontend.

---

# ✔️ Objetivo do Projeto

### Este projeto garante que:
- Arquivs baixando no dir FUGA (.tsv.gz), descompactação (.tsv) e Upload no S3 (.tsv.gz)
- Os arquivos (.tsv) foram processados aberto no Redis do **imusics-backend (Java)** pelo **im-symphonia-analytics (Python)**.
- Dados abertos no Redis agrupados e sumarizados pelo **im-symphonia-analytics (Python)** para o **imusics-backend (Java)** consumi-los.
- As métricas e agrupamentos estão consistentes com os dados brutos.
- Divergências entre: Redis (Dados Abertos) × Redis (Dados Sumarizados) são identificadas automaticamente.
- Dumps são gerados para análise rápida.

---

# 📂 Estrutura do Repositório
```bash
/src
/test
- analytics-process
  - DownloadUploadS3Test 
    - Garantir Download repo FUGA
    - Descompactações
    - Upload no S3 dos arquivos
  - UploadRedisOpenDataTest 
    - reimportações, Agrupamentos e sumarizações
    - Testes de consistência de Redis
    - Geração de dumps
/tmp/redis-dump
```

# 🧱 Arquitetura Validada pelo Projeto

                +-----------------------+
                |  Arquivos TSV (S3)    |
                +-----------+-----------+
                            |
                       Download / Sync
                            |
                            v
                +-----------------------+
                |  Analytics Updater    |
                |  (Python Microservice)|
                +-----------+-----------+
                            |
                Payload JSON por chunk
                            |
                            v
                +-----------------------+
                |         Redis         |
                |  Raw Rows / Hashes    |
                +-----------+-----------+
                            |
                Consumido pelo Backend
                            |
                            v
          +---------------------------------+
          | iMusics Backend (Quarkus/Java) |
          |   - Processa Rows              |
          |   - Gera Sumarizações          |
          |   - Grava chaves “imusic:*”    |
          +---------------------------------+
                            |
                            v
             +-----------------------------+
             |   Projeto de Testes (Kotlin)|
             | - Recalcula sumarização     |
             | - Compara Redis x Resultado |
             | - Gera dumps de divergência |
             +-----------------------------+

---

# 📦 Tecnologias Utilizadas

### Testes
- **Kotlin**
- **JUnit 5**
- **Gradle**
- **Docker** (para Redis local)
- **AWS S3** (origem dos arquivos)
- **Redis** (fonte de dados e sumarizações)

### Serviços validados por este projeto
- **Analytics Updater (Python)**
    - Processa TSV
    - Fatia arquivos
    - Envia JSONs para Redis
- **iMusics Backend (Java / Quarkus)**
    - Processa dados do Redis
    - Gera sumarizações
    - Armazena métricas em chaves `imusic:*`

---

# 🧪 O que este projeto valida

### 1. Gravação dos dados brutos (raw)
- Tipo da chave: `hash` / `list`
- `row_count`
- Conteúdo das linhas
- Status (`pending`, `processed`)

### 2. Processamento pelo backend
- Consumo das chaves de entrada
- Interpretação dos payloads
- Criação das chaves de sumarização

### 3. Sumarização
Os testes:

1. Buscam os dados crus do Redis
2. Determinam o tipo de agrupamento
3. Recalculam a sumarização em memória
4. Comparam com o valor gravado pelo backend
5. Validam chave a chave
6. Geram dumps detalhados quando necessário

---

# 🧮 Como funciona a validação da sumarização

Cada chave sumarizada, como por exemplo:`imusic:topregioes:Amazon:2025-09-30:rows`

Possui registros agrupados por campos como:

- artista
- região
- país
- label
- data de play
- loja/plataforma
- ISRC

O teste executa:

### ✓ 1. Carrega os dados crus do Redis
Esses dados vêm do Analytics Updater.

### ✓ 2. Identifica quais campos fazem parte do agrupamento
Ex.: `artist|country|region|play_date`

### ✓ 3. Recalcula o somatório de `number_of_streams`

### ✓ 4. Carrega do Redis o que o Backend gravou
Transforma em:

### ✓ 5. Compara quantidade de agrupamentos

### ✓ 6. Compara valores esperados × valores reais

### ✓ 7. Gera dumps:
`summaryKey_expected.json`
`summaryKey_from_redis.json`

---

# ⚠️ Exemplo real de divergência detectada
```bash
❌ Divergência → imusic:topregioes:Amazon:2025-09-30:rows
Chave: 1002877280734|DE|null|2025-09-30
Esperado: 0
Redis: null
```

Dados dos dumps:
```bash
**expected.json**
1002877280734|null|||2025-09-30 : 0

**redis.json**
1002877280734|Amazon|||2025-09-30 : 0
```
O teste identifica:

- Campos de agrupamento diferentes
- Valor ausente (`null`) no Redis
- Resultado inconsistente

---

# ▶️ Como rodar o projeto

## 1. Subir o projeto imusics-backend
```bash
make start-all
```
## 2. Subir o projeto im-symphonia-analytics
```bash
make start
```
## 3. Rodar os testes (EM CONTRUÇÃO)
```bash
./gradlew test
```

## 4. Como consultar dados direto no redis local
```bash
> Meta e o Agrupamento sumarizado
> Rows são as linhas

-- Install Redis
sudo apt install redis-tools

-- Logar no Redis Local
redis-cli
AUTH impass

-- Ver se existe (1 existe / 0 nao existe)
EXISTS imusic:topalbuns:Deezer:2025-09-23:rows
EXISTS imusic:topalbuns:Deezer:2025-09-23:meta


-- ver Ver o tipo primeiro
127.0.0.1:6379> TYPE imusic:dashes:Deezer:2025-09-23:meta
hash

-- hash
127.0.0.1:6379> HGETALL imusic:dashes:Deezer:2025-11-17:meta

-- String
GET imusic:dashes:Deezer:2025-11-17:meta

-- Lista
LLEN imusic:topplataform:Deezer:2025-09-23:rows

-- Ver primeiros 10 itens
LRANGE imusic:topplataform:Deezer:2025-09-23:rows 0 10

```






