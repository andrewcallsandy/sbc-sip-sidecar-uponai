const {
  JAMBONES_BLF_ENABLED,
  API_INTERNAL_BASE_URL,
  JAMBONES_INTERNAL_TOKEN,
} = require('./config');

const parseContactUser = (req) => {
  try {
    const uri = req.uri || '';
    // sip:blf-xxx@host or sips:...
    const m = /^(?:sips?:)?([^@;>]+)/i.exec(uri);
    if (m) return m[1];
    if (req.calledNumber) return req.calledNumber;
  } catch {
    // ignore
  }
  return null;
};

const parseCseq = (cseqHeader) => {
  if (!cseqHeader) return null;
  const n = parseInt(String(cseqHeader).split(/\s+/)[0], 10);
  return Number.isFinite(n) ? n : null;
};

/**
 * Inbound out-of-dialog NOTIFY handler for BLF Contacts (blf-*).
 * Always ACKs quickly; ingest is best-effort async.
 * Safe when BLF disabled: non-blf contacts get 404; blf contacts still 200 + skip ingest if disabled.
 */
module.exports = function createBlfNotifyHandler({logger, srf}) {
  return async function handleBlfNotify(req, res) {
    const contactUser = parseContactUser(req);
    if (!contactUser || !contactUser.startsWith('blf-')) {
      logger.debug({uri: req.uri}, 'NOTIFY ignored (not a BLF contact)');
      return res.send(404);
    }

    // ACK first — never block SIP on application work
    res.send(200);

    // One-shot RLS/directory probes (tools/blf-rls-probe.js) — log body, do not ingest
    if (contactUser.startsWith('blf-probe')) {
      const body = String(req.body || '');
      logger.info({
        contact_user: contactUser,
        content_type: req.get('Content-Type'),
        event: req.get('Event'),
        subscription_state: req.get('Subscription-State'),
        body_preview: body.slice(0, 800),
        looks_like_rls: /rlmi|resource-lists|multipart\/related/i.test(req.get('Content-Type') || '') ||
          /<resource-lists|<list\b|<rlmi\b/i.test(body),
      }, 'BLF RLS probe NOTIFY');
      return;
    }

    if (!JAMBONES_BLF_ENABLED) {
      logger.debug({contactUser}, 'BLF disabled; NOTIFY acknowledged but not ingested');
      return;
    }

    const base = (API_INTERNAL_BASE_URL || '').replace(/\/$/, '');
    if (!base || !JAMBONES_INTERNAL_TOKEN) {
      logger.error('BLF NOTIFY ingest skipped: API_INTERNAL_BASE_URL or JAMBONES_INTERNAL_TOKEN missing');
      return;
    }

    const payload = {
      contact_user: contactUser,
      source_ip: req.source_address,
      call_id: req.get('Call-ID'),
      cseq: parseCseq(req.get('CSeq')),
      event: req.get('Event'),
      subscription_state: req.get('Subscription-State'),
      content_type: req.get('Content-Type'),
      body: req.body || '',
      owner_node: srf.locals.regbot?.myToken || null,
    };

    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 5000);
      const response = await fetch(`${base}/v1/internal/Blf/notify`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${JAMBONES_INTERNAL_TOKEN}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      clearTimeout(timer);

      logger.info({
        contact_user: contactUser,
        source: payload.source_ip,
        event: payload.event,
        subscription_state: payload.subscription_state,
        api_status: response.status,
      }, 'BLF NOTIFY persisted');
    } catch (err) {
      logger.error({err: err.message, contactUser}, 'BLF NOTIFY ingest failed');
    }
  };
};
