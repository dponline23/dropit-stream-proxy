# DROPIT RU Stream Proxy v0.2

Purpose: keep the large supplier XML outside n8n memory. The proxy reads the source response as a stream, extracts one `<offer>` at a time, keeps only the RU text direction plus technical fields, and POSTs small batches to an n8n webhook.

## Text direction

This version is deliberately **RU only**:

- keeps unsuffixed `<name>`
- keeps unsuffixed `<description>`
- does **not** read `name_ua`, `name_uk`, `name_ukr`
- does **not** read `description_ua`, `description_uk`, `description_ukr`
- does not generate or copy RU text into any UA field

## What is kept

- `name`
- `description`
- price, currencyId, categoryId
- vendor, vendorCode
- barcode, country_of_origin
- stock quantity, weight
- first picture
- first 3 `<param>` tags
- offer `id` and `available`

## Endpoints

- `GET /health` — service health
- `POST /sync` — starts a background streaming import and immediately returns HTTP 202
- `GET /status` — progress / last error

If `SYNC_TOKEN` is configured, call `/sync` and `/status` with:

`Authorization: Bearer YOUR_TOKEN`

## n8n receiver workflow

Create a separate small workflow:

`Webhook (POST /dropit-ru-batch) -> Parse Offer Batch -> Split Out -> Google Sheets`

Use the Production URL of that Webhook as `N8N_WEBHOOK_URL`.

The proxy sends the same basic contract expected by the existing batch parser:

```json
{
  "jobId": "...",
  "batchIndex": 0,
  "offerAttrs": [" id=..."],
  "offerBodies": ["<name>...</name><description>...</description>..."],
  "categoriesById": {},
  "numberByOriginalId": {},
  "ruOnly": true
}
```

Start with `BATCH_SIZE=50`.

## Required small change in Parse Offer Batch

Do not let the existing fallback copy RU into UA columns. Use:

```js
const name = extractTagValue_(body, 'name');
const nameUa = '';
```

And fill only RU columns:

```js
row['Назва_позиції'] = name;
row['Назва_позиції_укр'] = '';

row['Опис'] = descPlain;
row['Опис_укр'] = '';

row['HTML_опис'] = descHtml;
row['HTML_опис_укр'] = '';
```

All technical fields stay unchanged.

## Trigger workflow in n8n

`Schedule Trigger -> HTTP Request POST https://YOUR-PROXY/sync`

Header:

`Authorization: Bearer <SYNC_TOKEN>`

The request gets HTTP 202 immediately. The proxy continues reading the large XML and pushes batches to the receiver workflow.

For debugging:

`GET https://YOUR-PROXY/status`

with the same Authorization header.

## Deploy

This project has no npm dependencies and requires Node.js 20+.

Set the environment variables from `.env.example`, then start with:

`npm start`
