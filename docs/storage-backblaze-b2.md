### Storage: Backblaze B2 (S3-compatible)

Use Backblaze B2 via the S3 API to store and serve files from the Circulation Manager.

- **Requisitos**:
  - Conta Backblaze B2 com S3 API habilitada
  - Application Key ID (access key) e Application Key (secret)
  - Bucket público para servir arquivos (ex.: `public`)

### Variáveis de ambiente

Defina estas variáveis (veja `.env.playlivros.example`):

- **PALACE_STORAGE_PUBLIC_ACCESS_BUCKET**: Nome do bucket público.
- **PALACE_STORAGE_ACCESS_KEY**: B2 Application Key ID.
- **PALACE_STORAGE_SECRET_KEY**: B2 Application Key.
- **PALACE_STORAGE_ENDPOINT_URL**: Endpoint S3 da região B2, com esquema. Ex.: `https://s3.us-east-005.backblazeb2.com`.
- **PALACE_STORAGE_URL_TEMPLATE**: Template de URL pública, use `{bucket}` e `{key}`. Ex.:
  - `https://s3.us-east-005.backblazeb2.com/{bucket}/{key}`
- **PALACE_STORAGE_REGION** (opcional): Prefira deixar em branco ao usar B2. Se precisar preencher:
  - Use `us-east-1` (compat) OU o código da sua região B2 (ex.: `us-west-004`). Ambos funcionam com `PALACE_STORAGE_ENDPOINT_URL`.
  - Observação: versões atuais do CM validam contra regiões da AWS; códigos B2 podem não ser aceitos. Se houver erro, deixe vazio ou use `us-east-1`.

### Exemplo rápido (.env)

```env
PALACE_STORAGE_PUBLIC_ACCESS_BUCKET=your-bucket-name
PALACE_STORAGE_ACCESS_KEY=YOUR_B2_KEY_ID
PALACE_STORAGE_SECRET_KEY=YOUR_B2_APPLICATION_KEY
PALACE_STORAGE_ENDPOINT_URL=https://s3.us-east-005.backblazeb2.com
PALACE_STORAGE_URL_TEMPLATE=https://s3.us-east-005.backblazeb2.com/{bucket}/{key}
# PALACE_STORAGE_REGION=us-east-1   # preferir deixar em branco; usar se necessário
# PALACE_STORAGE_REGION=us-west-004  # alternativo; pode falhar na validação do CM
```

### Observações

- **Bucket público**: certifique-se que o bucket configurado permite leitura pública dos objetos se pretende servir arquivos diretamente.
- **Segurança**: mantenha as chaves fora do controle de versão; use um gerenciador de segredos ou variáveis de ambiente seguras.
- **URLs geradas**: o CM usa o `PALACE_STORAGE_URL_TEMPLATE` para gerar links acessíveis publicamente.
