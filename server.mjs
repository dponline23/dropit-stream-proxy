import http from 'node:http';

const PROXY_VERSION = '0.4';

const PORT = Number(process.env.PORT || 3000);
const SOURCE_XML_URL = process.env.SOURCE_XML_URL || '';
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL || '';
const BATCH_SIZE = Math.max(1, Number(process.env.BATCH_SIZE || 50));
const SYNC_TOKEN = process.env.SYNC_TOKEN || '';
const N8N_WEBHOOK_AUTH_HEADER = process.env.N8N_WEBHOOK_AUTH_HEADER || '';
const N8N_WEBHOOK_AUTH_VALUE = process.env.N8N_WEBHOOK_AUTH_VALUE || '';

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

  const cdata = name.match(
    /^<!\[CDATA\[([\s\S]*?)\]\]>$/,
  );

  if (cdata) {
    name = cdata[1];
  }

  name = name
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  return {
    id,
    parentId,
    name,
  };
}

function buildOfferPayload(offerXml) {
  const open = offerXml.match(/^<offer\b([^>]*)>/i);

  if (!open) {
    return null;
  }

  /*
   * v0.4
   *
   * НІЧОГО всередині offer більше не вирізаємо.
   *
   * Передаємо повністю:
   *
   * name
   * name_ua
   * name_uk
   * description
   * description_ua
   * description_uk
   * price
   * currencyId
   * categoryId
   * vendor
   * vendorCode
   * barcode
   * quantity
   * ВСІ picture
   * ВСІ param
   * та будь-які інші поля постачальника.
   */

  const attrs = open[1] || '';

  const body = offerXml
    .slice(open[0].length)
    .replace(/<\/offer>\s*$/i, '');

  return {
    attrs,
    body,
  };
}

async function postBatch(payload) {
  const headers = {
    'content-type': 'application/json',
  };

  if (
    N8N_WEBHOOK_AUTH_HEADER &&
    N8N_WEBHOOK_AUTH_VALUE
  ) {
    headers[N8N_WEBHOOK_AUTH_HEADER] =
      N8N_WEBHOOK_AUTH_VALUE;
  }

  let lastError;

  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const response = await fetch(
        N8N_WEBHOOK_URL,
        {
          method: 'POST',
          headers,
          body: JSON.stringify(payload),
        },
      );

      if (!response.ok) {
        const text = await response
          .text()
          .catch(() => '');

        throw new Error(
          `n8n webhook ${response.status}: ${text.slice(
            0,
            500,
          )}`,
        );
      }

      return;
    } catch (err) {
      lastError = err;

      if (attempt < 4) {
        await new Promise((resolve) =>
          setTimeout(
            resolve,
            1000 * 2 ** (attempt - 1),
          ),
        );
      }
    }
  }

  throw lastError;
}

async function runSync(jobId) {
  if (!SOURCE_XML_URL) {
    throw new Error(
      'SOURCE_XML_URL is not configured',
    );
  }

  if (!N8N_WEBHOOK_URL) {
    throw new Error(
      'N8N_WEBHOOK_URL is not configured',
    );
  }

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

  /*
   * Категорії читаємо один раз на початку XML.
   */

  const categoriesById =
    Object.create(null);

  const numberByOriginalId =
    Object.create(null);

  let categoryNumber = 0;

  /*
   * Поточний batch.
   */

  let offerAttrs = [];
  let offerBodies = [];

  let batchIndex = 0;

  async function flushBatch() {
    if (offerBodies.length === 0) {
      return;
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
    });

    state.offersSent += count;
    state.batchesSent += 1;
    state.lastBatchAt = nowIso();

    batchIndex += 1;

    /*
     * Старий batch звільняємо.
     */

    offerAttrs = [];
    offerBodies = [];
  }

  /*
   * Головний запит до DropIT.
   *
   * ВАЖЛИВО:
   * response.text() тут НЕ використовується.
   *
   * XML читається потоком.
   */

  const response = await fetch(
    SOURCE_XML_URL,
    {
      method: 'GET',

      headers: {
        accept:
          'application/xml,text/xml,*/*',

        'user-agent':
          `DROPIT-Stream-Proxy/${PROXY_VERSION}`,
      },

      redirect: 'follow',
    },
  );

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

  const reader =
    response.body.getReader();

  const decoder =
    new TextDecoder('utf-8');

  /*
   * Тут знаходиться лише невелика частина XML.
   */

  let buffer = '';

  /*
   * До першого offer збираємо categories.
   */

  let offersStarted = false;

  async function processBuffer() {
    while (true) {
      /*
       * --------------------------------
       * CATEGORY MODE
       * --------------------------------
       */

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

        /*
         * Якщо category знаходиться
         * раніше першого offer.
         */

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

          /*
           * category ще не повністю
           * прийшла з мережі.
           */

          if (end < 0) {
            if (catPos > 0) {
              buffer =
                buffer.slice(catPos);
            }

            return;
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

        /*
         * Дійшли до товарів.
         */

        if (offerPos >= 0) {
          buffer =
            buffer.slice(
              offerPos,
            );

          offersStarted = true;

          continue;
        }

        /*
         * Щоб службова частина XML
         * до offers не накопичувалась.
         */

        if (buffer.length > 8192) {
          buffer =
            buffer.slice(-8192);
        }

        return;
      }

      /*
       * --------------------------------
       * OFFER MODE
       * --------------------------------
       */

      const startMatch =
        buffer.match(/<offer\b/i);

      /*
       * Початку offer ще немає.
       */

      if (!startMatch) {
        /*
         * Залишаємо невеликий хвіст,
         * якщо "<offer" розірвало
         * між двома network chunk.
         */

        if (buffer.length > 64) {
          buffer =
            buffer.slice(-64);
        }

        return;
      }

      const start =
        startMatch.index;

      /*
       * Все перед offer вже не потрібне.
       */

      if (start > 0) {
        buffer =
          buffer.slice(start);
      }

      /*
       * Шукаємо кінець товару.
       */

      const end =
        buffer.indexOf(
          '</offer>',
        );

      /*
       * Offer ще не прийшов повністю.
       *
       * Зберігаємо його до наступного
       * network chunk.
       */

      if (end < 0) {
        return;
      }

      /*
       * Вирізаємо ОДИН повний offer.
       */

      const offerXml =
        buffer.slice(
          0,
          end +
            '</offer>'.length,
        );

      /*
       * Видаляємо його з буфера.
       */

      buffer =
        buffer.slice(
          end +
            '</offer>'.length,
        );

      state.offersSeen += 1;

      /*
       * У v0.4 buildOfferPayload
       * НЕ вирізає поля.
       *
       * Зберігається все тіло offer.
       */

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

      /*
       * Batch готовий.
       */

      if (
        offerBodies.length >=
        BATCH_SIZE
      ) {
        /*
         * Backpressure.
         *
         * Поки n8n не прийняв batch,
         * наступну частину XML
         * активно не читаємо.
         */

        await flushBatch();
      }
    }
  }

  /*
   * --------------------------------
   * STREAM READER
   * --------------------------------
   */

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

    await processBuffer();
  }

  /*
   * Фінальний хвіст UTF-8.
   */

  buffer += decoder.decode();

  await processBuffer();

  /*
   * Останній batch.
   *
   * Наприклад:
   * 44 товари замість 100.
   */

  await flushBatch();

  /*
   * --------------------------------
   * RESULT
   * --------------------------------
   */

  state.lastResult = {
    jobId,

    proxyVersion:
      PROXY_VERSION,

    payloadMode:
      'full-offer-body',

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

function startSync() {
  const jobId =
    `${Date.now()}-${Math.random()
      .toString(36)
      .slice(2, 8)}`;

  /*
   * runSync працює у фоні.
   *
   * POST /sync відразу повертає
   * accepted=true.
   */

  void runSync(jobId)
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
      state.finishedAt =
        nowIso();
    });

  return jobId;
}

/*
 * --------------------------------
 * HTTP SERVER
 * --------------------------------
 */

const server =
  http.createServer(
    (req, res) => {
      const url =
        new URL(
          req.url || '/',
          `http://${
            req.headers.host ||
            'localhost'
          }`,
        );

      /*
       * HEALTH
       */

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

      /*
       * AUTH
       */

      if (
        (
          url.pathname ===
            '/sync' ||
          url.pathname ===
            '/status'
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

      /*
       * STATUS
       */

      if (
        req.method === 'GET' &&
        url.pathname ===
          '/status'
      ) {
        return json(
          res,
          200,
          publicState(),
        );
      }

      /*
       * SYNC
       */

      if (
        req.method === 'POST' &&
        url.pathname ===
          '/sync'
      ) {
        /*
         * Другий sync одночасно
         * не запускаємо.
         */

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
          startSync();

        return json(
          res,
          202,
          {
            accepted: true,

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

      /*
       * 404
       */

      return json(
        res,
        404,
        {
          error:
            'Not found',

          endpoints: [
            'GET /health',
            'POST /sync',
            'GET /status',
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
