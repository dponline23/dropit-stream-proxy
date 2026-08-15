import http from 'node:http';

const PORT = Number(process.env.PORT || 3000);
const SOURCE_XML_URL = process.env.SOURCE_XML_URL || '';
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL || '';
const BATCH_SIZE = Math.max(1, Number(process.env.BATCH_SIZE || 50));
const SYNC_TOKEN = process.env.SYNC_TOKEN || '';
const N8N_WEBHOOK_AUTH_HEADER = process.env.N8N_WEBHOOK_AUTH_HEADER || '';
const N8N_WEBHOOK_AUTH_VALUE = process.env.N8N_WEBHOOK_AUTH_VALUE || '';
const MAX_PARAMS = 3;

const state = {
  running: false,
  jobId: null,
  startedAt: null,
  finishedAt: null,
  offersSeen: 0,
  offersSent: 0,
  batchesSent: 0,
  bytesRead: 0,
  categories: 0,
  lastBatchAt: null,
  lastError: null,
  lastResult: null,
};

function nowIso() {
  return new Date().toISOString();
}

function publicState() {
  return { ...state };
}

function json(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(data),
    'cache-control': 'no-store',
  });
  res.end(data);
}

function isAuthorized(req) {
  if (!SYNC_TOKEN) return true;
  const auth = req.headers.authorization || '';
  return auth === `Bearer ${SYNC_TOKEN}`;
}

function attrValue(attrs, name) {
  if (!attrs) return '';
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  let m = attrs.match(new RegExp(`${escaped}\\s*=\\s*"([^"]*)"`, 'i'));
  if (m) return m[1];
  m = attrs.match(new RegExp(`${escaped}\\s*=\\s*'([^']*)'`, 'i'));
  return m ? m[1] : '';
}

function tagInner(body, tagName) {
  const m = body.match(new RegExp(`<${tagName}\\b[^>]*>([\\s\\S]*?)<\\/${tagName}>`, 'i'));
  return m ? m[1] : '';
}

function fullTag(body, tagName) {
  const m = body.match(new RegExp(`<${tagName}\\b[^>]*>[\\s\\S]*?<\\/${tagName}>`, 'i'));
  return m ? m[0] : '';
}

function firstParams(body, max = MAX_PARAMS) {
  const out = [];
  const re = /<param\b[^>]*>[\s\S]*?<\/param>/gi;
  let m;
  while (out.length < max && (m = re.exec(body)) !== null) out.push(m[0]);
  return out;
}

function escapeXmlAttr(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function parseCategory(categoryXml) {
  const open = categoryXml.match(/^<category\b([^>]*)>/i);
  if (!open) return null;
  const attrs = open[1] || '';
  const id = attrValue(attrs, 'id');
  if (!id) return null;
  const parentId = attrValue(attrs, 'parentId');
  let name = categoryXml
    .replace(/^<category\b[^>]*>/i, '')
    .replace(/<\/category>\s*$/i, '')
    .trim();
  const cdata = name.match(/^<!\[CDATA\[([\s\S]*?)\]\]>$/);
  if (cdata) name = cdata[1];
  name = name.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  return { id, parentId, name };
}

function buildLightOffer(offerXml) {
  const open = offerXml.match(/^<offer\b([^>]*)>/i);
  if (!open) return null;

  const attrs = open[1] || '';
  const body = offerXml
    .slice(open[0].length)
    .replace(/<\/offer>\s*$/i, '');

  const id = attrValue(attrs, 'id');
  const availableAttr = attrValue(attrs, 'available');

  // Keep RU and UA independently. No cross-language fallback.
  // UA source may use either *_ua or *_uk, so normalize it to *_ua downstream.
  const nameRu = tagInner(body, 'name');
  const nameUa = tagInner(body, 'name_ua') || tagInner(body, 'name_uk');
  const descriptionRu = tagInner(body, 'description');
  const descriptionUa = tagInner(body, 'description_ua') || tagInner(body, 'description_uk');

  const parts = [];

  if (nameRu !== '') parts.push(`<name>${nameRu}</name>`);
  if (nameUa !== '') parts.push(`<name_ua>${nameUa}</name_ua>`);
  if (descriptionRu !== '') parts.push(`<description>${descriptionRu}</description>`);
  if (descriptionUa !== '') parts.push(`<description_ua>${descriptionUa}</description_ua>`);

  const technicalTags = [
    'available',
    'price',
    'currencyId',
    'categoryId',
    'vendor',
    'vendorCode',
    'barcode',
    'country_of_origin',
    'quantity_in_stock',
    'stock_quantity',
    'weight',
    'weight_kg',
  ];

  for (const tag of technicalTags) {
    const value = fullTag(body, tag);
    if (value) parts.push(value);
  }

  // Existing n8n parser only uses the first picture anyway.
  const picture = fullTag(body, 'picture');
  if (picture) parts.push(picture);

  parts.push(...firstParams(body));

  let lightAttrs = '';
  if (id) lightAttrs += ` id="${escapeXmlAttr(id)}"`;
  if (availableAttr) lightAttrs += ` available="${escapeXmlAttr(availableAttr)}"`;

  return {
    attrs: lightAttrs,
    body: parts.join(''),
  };
}

async function postBatch(payload) {
  const headers = { 'content-type': 'application/json' };
  if (N8N_WEBHOOK_AUTH_HEADER && N8N_WEBHOOK_AUTH_VALUE) {
    headers[N8N_WEBHOOK_AUTH_HEADER] = N8N_WEBHOOK_AUTH_VALUE;
  }

  let lastError;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const response = await fetch(N8N_WEBHOOK_URL, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        const text = await response.text().catch(() => '');
        throw new Error(`n8n webhook ${response.status}: ${text.slice(0, 500)}`);
      }
      return;
    } catch (err) {
      lastError = err;
      if (attempt < 4) {
        await new Promise((resolve) => setTimeout(resolve, 1000 * 2 ** (attempt - 1)));
      }
    }
  }
  throw lastError;
}

async function runSync(jobId) {
  if (!SOURCE_XML_URL) throw new Error('SOURCE_XML_URL is not configured');
  if (!N8N_WEBHOOK_URL) throw new Error('N8N_WEBHOOK_URL is not configured');

  state.running = true;
  state.jobId = jobId;
  state.startedAt = nowIso();
  state.finishedAt = null;
  state.offersSeen = 0;
  state.offersSent = 0;
  state.batchesSent = 0;
  state.bytesRead = 0;
  state.categories = 0;
  state.lastBatchAt = null;
  state.lastError = null;
  state.lastResult = null;

  const categoriesById = Object.create(null);
  const numberByOriginalId = Object.create(null);
  let categoryNumber = 0;

  let offerAttrs = [];
  let offerBodies = [];
  let batchIndex = 0;

  const flushBatch = async () => {
    if (offerBodies.length === 0) return;
    const count = offerBodies.length;

    await postBatch({
      jobId,
      batchIndex,
      offerAttrs,
      offerBodies,
      categoriesById,
      numberByOriginalId,
      languageMode: 'ru+ua',
    });

    state.offersSent += count;
    state.batchesSent += 1;
    state.lastBatchAt = nowIso();
    batchIndex += 1;
    offerAttrs = [];
    offerBodies = [];
  };

  const response = await fetch(SOURCE_XML_URL, {
    method: 'GET',
    headers: {
      'accept': 'application/xml,text/xml,*/*',
      'user-agent': 'DROPIT-Stream-Proxy/0.3',
    },
    redirect: 'follow',
  });

  if (!response.ok) {
    throw new Error(`Source XML HTTP ${response.status} ${response.statusText}`);
  }
  if (!response.body) throw new Error('Source response has no readable body');

  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';
  let offersStarted = false;

  const processBuffer = async (final = false) => {
    while (true) {
      if (!offersStarted) {
        const offerMatch = buffer.match(/<offer\b/i);
        const offerPos = offerMatch ? offerMatch.index : -1;
        const catMatch = buffer.match(/<category\b/i);
        const catPos = catMatch ? catMatch.index : -1;

        if (catPos >= 0 && (offerPos < 0 || catPos < offerPos)) {
          const end = buffer.indexOf('</category>', catPos);
          if (end < 0) {
            if (catPos > 0) buffer = buffer.slice(catPos);
            return;
          }
          const xml = buffer.slice(catPos, end + '</category>'.length);
          const cat = parseCategory(xml);
          if (cat && !categoriesById[cat.id]) {
            categoriesById[cat.id] = cat;
            categoryNumber += 1;
            numberByOriginalId[cat.id] = categoryNumber;
            state.categories = categoryNumber;
          }
          buffer = buffer.slice(end + '</category>'.length);
          continue;
        }

        if (offerPos >= 0) {
          buffer = buffer.slice(offerPos);
          offersStarted = true;
          continue;
        }

        // Keep only a small tail for a tag that may be split across chunks.
        if (buffer.length > 8192) buffer = buffer.slice(-8192);
        return;
      }

      const startMatch = buffer.match(/<offer\b/i);
      if (!startMatch) {
        if (buffer.length > 64) buffer = buffer.slice(-64);
        return;
      }
      const start = startMatch.index;
      if (start > 0) buffer = buffer.slice(start);

      const end = buffer.indexOf('</offer>');
      if (end < 0) {
        // One incomplete offer may remain here. This is bounded by the size of
        // a single offer, rather than the 167 MB source document.
        return;
      }

      const offerXml = buffer.slice(0, end + '</offer>'.length);
      buffer = buffer.slice(end + '</offer>'.length);
      state.offersSeen += 1;

      const light = buildLightOffer(offerXml);
      if (light) {
        offerAttrs.push(light.attrs);
        offerBodies.push(light.body);
      }

      if (offerBodies.length >= BATCH_SIZE) {
        // Backpressure: do not read more XML while n8n is processing the batch.
        await flushBatch();
      }
    }
  };

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    state.bytesRead += value.byteLength;
    buffer += decoder.decode(value, { stream: true });
    await processBuffer(false);
  }

  buffer += decoder.decode();
  await processBuffer(true);
  await flushBatch();

  state.lastResult = {
    jobId,
    offersSeen: state.offersSeen,
    offersSent: state.offersSent,
    batchesSent: state.batchesSent,
    categories: state.categories,
    bytesRead: state.bytesRead,
  };
}

function startSync() {
  const jobId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  // Intentionally detached from the HTTP response. On a normal persistent Node
  // web service (Railway/Render), /sync returns immediately while this continues.
  void runSync(jobId)
    .catch((err) => {
      state.lastError = {
        at: nowIso(),
        message: err instanceof Error ? err.message : String(err),
      };
    })
    .finally(() => {
      state.running = false;
      state.finishedAt = nowIso();
    });

  return jobId;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'GET' && url.pathname === '/health') {
    return json(res, 200, { ok: true, running: state.running });
  }

  if ((url.pathname === '/sync' || url.pathname === '/status') && !isAuthorized(req)) {
    return json(res, 401, { error: 'Unauthorized' });
  }

  if (req.method === 'GET' && url.pathname === '/status') {
    return json(res, 200, publicState());
  }

  if (req.method === 'POST' && url.pathname === '/sync') {
    if (state.running) {
      return json(res, 409, {
        error: 'Sync already running',
        jobId: state.jobId,
        status: publicState(),
      });
    }

    const jobId = startSync();
    return json(res, 202, {
      accepted: true,
      jobId,
      batchSize: BATCH_SIZE,
      languageMode: 'ru+ua',
    });
  }

  return json(res, 404, {
    error: 'Not found',
    endpoints: ['GET /health', 'POST /sync', 'GET /status'],
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`DROPIT stream proxy listening on 0.0.0.0:${PORT}`);
});
