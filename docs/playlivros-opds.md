### OPDS2 para PlayLivros

OPDS2 é um formato de feed em JSON para distribuição de publicações digitais. O Circulation Manager (CM) pode importar coleções a partir de um URL de feed OPDS2.

Neste fluxo, os arquivos EPUB ficam no Backblaze B2 (S3-compatible) e o feed OPDS2 apenas referencia URLs públicas desses objetos.

### Variáveis de ambiente para o gerador

- B2_ENDPOINT: s3.us-east-005.backblazeb2.com
- B2_KEY_ID: YOUR_B2_KEY_ID
- B2_APP_KEY: YOUR_B2_APPLICATION_KEY
- B2_BUCKET: PlayLivros
- B2_PREFIX: (ex.: ebooks/)

Exemplo de uso (bash):
```bash
export B2_ENDPOINT=s3.us-east-005.backblazeb2.com
export B2_KEY_ID=YOUR_B2_KEY_ID
export B2_APP_KEY=YOUR_B2_APPLICATION_KEY
export B2_BUCKET=PlayLivros
export B2_PREFIX=ebooks/
make opds
```

### Exemplo de entry OPDS2 (publicação)

```json
{
  "metadata": { "title": "Título do EPUB" },
  "links": [
    {
      "rel": "http://opds-spec.org/acquisition/open-access",
      "type": "application/epub+zip",
      "href": "https://s3.us-west-004.backblazeb2.com/<bucket>/<key>"
    }
  ]
}
```

### Como usar

1. Defina as variáveis de ambiente acima.
2. Rode `make opds` para gerar `public/feed/opds2-playlivros.json`.
3. Para testar localmente, rode `make opds-serve` e acesse `http://localhost:8080/feed/opds2-playlivros.json`.
