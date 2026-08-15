import http from 'node:http';

const PROXY_VERSION = '0.4.1';
const PORT = Number(process.env.PORT || 3000);
const SOURCE_XML_URL = process.env.SOURCE_XML_URL || '';
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL || '';
const BATCH_SIZE = Math.max(1, Number(process.env.BATCH_SIZE || 50));
const SYNC_TOKEN = process.env.SYNC_TOKEN || '';
const N8N_WEBHOOK_AUTH_HEADER = process.env.N8N_WEBHOOK_AUTH_HEADER || '';
const N8N_WEBHOOK_AUTH_VALUE = process.env.N8N_WEBHOOK_AUTH_VALUE || '';

const state = {
  running: false,
  mode: null,
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
  return {
    proxyVersion: PROXY_VERSION,
    payloadMode: 'full-offer-body',
    ...state,
  };
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

  let m = attrs.match(
    new RegExp(`${escaped}\\s*=\\s*"([^"]*)"`, 'i'),
  );

  if (m) return m[1];

  m = attrs.match(
    new RegExp(`${escaped}\\s*=\\s*'([^']*)'`, 'i'),
  );

  return m ? m[1] : '';
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

  name = name
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  return { id, parentId, name };
}

function buildOfferPayload(offerXml) {
  const open = offerXml.match(/^<offer\b([^>]*)>/i);
  if (!open) return null;

  // v0.4.1: передаємо ВСІ атрибути offer і ПОВНЕ внутрішнє XML.
  // Тобто не губляться picture, categoryId, vendor, param, RU/UA поля тощо.
  const attrs = open[1] || '';

  const body = offerXml
    .slice(open[0].length)
    .replace(/<\/offer>\s*$/i, '');

  return { attrs, body };
}

async function postBatch(payload) {
  const headers = {
    'content-type': 'application/json',
  };

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

        throw new Error(
          `n8n webhook ${response.status}: ${text.slice(0, 500)}`,
        );
      }

      return;
    } catch (err) {
      lastError = err;

      if (attempt < 4) {
        await new Promise((resolve) =>
          setTimeout(resolve, 1000 * 2 ** (attempt - 1)),
        );
      }
    }
  }

  throw lastError;
}

async function runSync(jobId, options = {}) {
  const mode = options.mode || 'full';

  const maxBatches = Number.isFinite(options.maxBatches)
    ? Math.max(1, Number(options.maxBatches))
    : null;

  if (!SOURCE_XML_URL) {
    throw new Error('SOURCE_XML_URL is not configured');
  }

  if (!N8N_WEBHOOK_URL) {
    throw new Error('N8N_WEBHOOK_URL is not configured');
  }

  state.running = true;
  state.mode = mode;
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

  let stoppedByBatchLimit = false;

  const flushBatch = async () => {
    if (offerBodies.length === 0) {
      return false;
    }

    const count = offerBodies.length;

    await postBatch({
      jobId,
      batchIndex,

      offerAttrs,
      offerBodies,

      categoriesById,
      numberByOriginalId,

      languageMode: 'ru+ua',

      proxyVersion: PROXY_VERSION,
      payloadMode: 'full-offer-body',
      syncMode: mode,
    });

    state.offersSent += count;
    state.batchesSent += 1;
    state.lastBatchAt = nowIso();

    batchIndex += 1;

    offerAttrs = [];
    offerBodies = [];

    if (
      maxBatches !== null &&
      state.batchesSent >= maxBatches
    ) {
      stoppedByBatchLimit = true;
      return true;
    }

    return false;
  };

  const response = await fetch(SOURCE_XML_URL, {
    method: 'GET',

    headers: {
      accept: 'application/xml,text/xml,*/*',

      'user-agent':
        `DROPIT-Stream-Proxy/${PROXY_VERSION}`,
    },

    redirect: 'follow',
  });

  if (!response.ok) {
    throw new Error(
      `Source XML HTTP ${response.status} ${response.statusText}`,
    );
  }

  if (!response.body) {
    throw new Error(
      'Source response has no readable body',
    );
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8');

  let buffer = '';
  let offersStarted = false;

  const processBuffer = async () => {
    while (true) {
      if (!offersStarted) {
        const offerMatch =
          buffer.match(/<offer\b/i);

        const offerPos =
          offerMatch
            ? offerMatch.index
            : -1;

        const catMatch =
          buffer.match(/<category\b/i);

        const catPos =
          catMatch
            ? catMatch.index
            : -1;

        if (
          catPos >= 0 &&
          (
            offerPos < 0 ||
            catPos < offerPos
          )
        ) {
          const end =
            buffer.indexOf(
              '</category>',
              catPos,
            );

          if (end < 0) {
            if (catPos > 0) {
              buffer =
                buffer.slice(catPos);
            }

            return false;
          }

          const categoryXml =
            buffer.slice(
              catPos,
              end +
                '</category>'.length,
            );

          const category =
            parseCategory(
              categoryXml,
            );

          if (
            category &&
            !categoriesById[
              category.id
            ]
          ) {
            categoriesById[
              category.id
            ] = category;

            categoryNumber += 1;

            numberByOriginalId[
              category.id
            ] = categoryNumber;

            state.categories =
              categoryNumber;
          }

          buffer =
            buffer.slice(
              end +
                '</category>'.length,
            );

          continue;
        }

        if (offerPos >= 0) {
          buffer =
            buffer.slice(
              offerPos,
            );

          offersStarted = true;

          continue;
        }

        if (buffer.length > 8192) {
          buffer =
            buffer.slice(-8192);
        }

        return false;
      }

      const startMatch =
        buffer.match(/<offer\b/i);

      if (!startMatch) {
        if (buffer.length > 64) {
          buffer =
            buffer.slice(-64);
        }

        return false;
      }

      const start =
        startMatch.index;

      if (start > 0) {
        buffer =
          buffer.slice(start);
      }

      const end =
        buffer.indexOf(
          '</offer>',
        );

      if (end < 0) {
        return false;
      }

      const offerXml =
        buffer.slice(
          0,
          end +
            '</offer>'.length,
        );

      buffer =
        buffer.slice(
          end +
            '</offer>'.length,
        );

      state.offersSeen += 1;

      const offer =
        buildOfferPayload(
          offerXml,
        );

      if (offer) {
        offerAttrs.push(
          offer.attrs,
        );

        offerBodies.push(
          offer.body,
        );
      }

      if (
        offerBodies.length >=
        BATCH_SIZE
      ) {
        const shouldStop =
          await flushBatch();

        if (shouldStop) {
          return true;
        }
      }
    }
  };

  while (true) {
    const {
      value,
      done,
    } = await reader.read();

    if (done) {
      break;
    }

    state.bytesRead +=
      value.byteLength;

    buffer +=
      decoder.decode(
        value,
        {
          stream: true,
        },
      );

    const shouldStop =
      await processBuffer();

    if (shouldStop) {
      await reader
        .cancel(
          'sync batch limit reached',
        )
        .catch(() => {});

      buffer = '';

      break;
    }
  }

  if (!stoppedByBatchLimit) {
    buffer += decoder.decode();

    const shouldStop =
      await processBuffer();

    if (!shouldStop) {
      await flushBatch();
    }
  }

  state.lastResult = {
    jobId,

    proxyVersion:
      PROXY_VERSION,

    payloadMode:
      'full-offer-body',

    syncMode:
      mode,

    maxBatches,

    stoppedByBatchLimit,

    offersSeen:
      state.offersSeen,

    offersSent:
      state.offersSent,

    batchesSent:
      state.batchesSent,

    categories:
      state.categories,

    bytesRead:
      state.bytesRead,
  };
}

function startSync(options = {}) {
  const jobId =
    `${Date.now()}-${Math.random()
      .toString(36)
      .slice(2, 8)}`;

  void runSync(
    jobId,
    options,
  )
    .catch((err) => {
      state.lastError = {
        at: nowIso(),

        message:
          err instanceof Error
            ? err.message
            : String(err),
      };
    })
    .finally(() => {
      state.running = false;
      state.finishedAt = nowIso();
    });

  return jobId;
}

const server =
  http.createServer(
    (req, res) => {
      const url =
        new URL(
          req.url || '/',
          `http://${req.headers.host || 'localhost'}`,
        );

      if (
        req.method === 'GET' &&
        url.pathname === '/health'
      ) {
        return json(
          res,
          200,
          {
            ok: true,

            running:
              state.running,

            proxyVersion:
              PROXY_VERSION,

            payloadMode:
              'full-offer-body',
          },
        );
      }

      if (
        (
          url.pathname === '/sync' ||
          url.pathname === '/sync-test' ||
          url.pathname === '/status'
        ) &&
        !isAuthorized(req)
      ) {
        return json(
          res,
          401,
          {
            error:
              'Unauthorized',
          },
        );
      }

      if (
        req.method === 'GET' &&
        url.pathname === '/status'
      ) {
        return json(
          res,
          200,
          publicState(),
        );
      }

      if (
        req.method === 'POST' &&
        url.pathname === '/sync-test'
      ) {
        if (state.running) {
          return json(
            res,
            409,
            {
              error:
                'Sync already running',

              jobId:
                state.jobId,

              status:
                publicState(),
            },
          );
        }

        const jobId =
          startSync({
            mode: 'test',
            maxBatches: 1,
          });

        return json(
          res,
          202,
          {
            accepted: true,

            testMode: true,

            jobId,

            batchSize:
              BATCH_SIZE,

            maxBatches: 1,

            expectedN8nWebhookExecutions: 1,

            languageMode:
              'ru+ua',

            proxyVersion:
              PROXY_VERSION,

            payloadMode:
              'full-offer-body',
          },
        );
      }

      if (
        req.method === 'POST' &&
        url.pathname === '/sync'
      ) {
        if (state.running) {
          return json(
            res,
            409,
            {
              error:
                'Sync already running',

              jobId:
                state.jobId,

              status:
                publicState(),
            },
          );
        }

        const jobId =
          startSync({
            mode: 'full',
          });

        return json(
          res,
          202,
          {
            accepted: true,

            testMode: false,

            jobId,

            batchSize:
              BATCH_SIZE,

            languageMode:
              'ru+ua',

            proxyVersion:
              PROXY_VERSION,

            payloadMode:
              'full-offer-body',
          },
        );
      }

      return json(
        res,
        404,
        {
          error:
            'Not found',

          endpoints: [
            'GET /health',
            'GET /status',
            'POST /sync-test',
            'POST /sync',
          ],
        },
      );
    },
  );

server.listen(
  PORT,
  '0.0.0.0',
  () => {
    console.log(
      `DROPIT stream proxy v${PROXY_VERSION} listening on 0.0.0.0:${PORT}`,
    );
  },
);
