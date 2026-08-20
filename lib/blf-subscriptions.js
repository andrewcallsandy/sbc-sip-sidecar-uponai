const {
  JAMBONES_BLF_ENABLED,
  JAMBONES_BLF_RECONCILE_SECONDS,
  JAMBONES_BLF_STATE_LEAD_SECONDS,
  API_INTERNAL_BASE_URL,
  JAMBONES_INTERNAL_TOKEN,
  JAMBONES_REGBOT_USER_AGENT,
  JAMBONES_REGBOT_CONTACT_USE_IP,
} = require('./config');
const version = require('../package.json').version;
const useragent = JAMBONES_REGBOT_USER_AGENT || `Jambonz ${version}`;

const RECONCILE_MS = (parseInt(JAMBONES_BLF_RECONCILE_SECONDS, 10) || 30) * 1000;
const STATE_LEAD_MS = (parseInt(JAMBONES_BLF_STATE_LEAD_SECONDS, 10) || 60) * 1000;

let timer = null;
let running = false;
let stopped = true;

const apiBase = () => (API_INTERNAL_BASE_URL || '').replace(/\/$/, '');

async function apiCall(path, opts = {}) {
  const base = apiBase();
  if (!base || !JAMBONES_INTERNAL_TOKEN) {
    throw new Error('API_INTERNAL_BASE_URL or JAMBONES_INTERNAL_TOKEN not configured');
  }
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 10000);
  try {
    const res = await fetch(`${base}${path}`, {
      ...opts,
      headers: {
        Authorization: `Bearer ${JAMBONES_INTERNAL_TOKEN}`,
        'Content-Type': 'application/json',
        ...(opts.headers || {}),
      },
      signal: controller.signal,
    });
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch { json = null; }
    if (!res.ok) {
      const err = new Error(`BLF API ${path} => ${res.status}`);
      err.status = res.status;
      err.body = json || text;
      throw err;
    }
    return json;
  } finally {
    clearTimeout(t);
  }
}

function isCarrierRegistered(carrier) {
  // regbot writes { status: 'ok'|'fail', reason, ... }
  const status = carrier?.register_status;
  if (!status) return false;
  const o = typeof status === 'string' ? (() => {
    try { return JSON.parse(status); } catch { return {}; }
  })() : status;
  return o.status === 'ok' || o.status === 'registered';
}

function needsRefresh(monitor, subscribeExpires, staleSeconds) {
  if (!monitor.sub_status || monitor.sub_status === 'none' || monitor.sub_status === 'terminated' ||
      monitor.sub_status === 'error') {
    return true;
  }
  const now = Date.now();
  if (monitor.sub_expires_at) {
    const exp = new Date(monitor.sub_expires_at).getTime();
    if (!Number.isNaN(exp) && exp - now <= STATE_LEAD_MS) return true;
  }
  if (monitor.stale_at) {
    const stale = new Date(monitor.stale_at).getTime();
    if (!Number.isNaN(stale) && stale - now <= STATE_LEAD_MS) return true;
  } else if (monitor.sub_status === 'active') {
    // no stale_at yet — refresh on interval based on subscribe expires
    return false;
  }
  // unused vars kept for future tuning
  void subscribeExpires;
  void staleSeconds;
  return false;
}

function buildContact(srf, contactUser, transport, scheme) {
  const publicAddress = (srf.locals.sbcPublicIpAddress?.udp || '').split(':')[0]
    || (srf.locals.sbcPublicIpAddress?.tls || '').split(':')[0];
  const host = JAMBONES_REGBOT_CONTACT_USE_IP
    ? publicAddress
    : (srf.locals.localSIPDomain || publicAddress);
  return `<${scheme}:${contactUser}@${host};transport=${transport}>`;
}

async function upsertSubscription(body) {
  return apiCall('/v1/internal/Blf/subscription', {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

async function sendSubscribe(logger, srf, {cfg, monitor, expires}) {
  const carrier = cfg.carrier;
  const realm = carrier.register_sip_realm || '';
  const fromUser = carrier.register_from_user || carrier.register_username;
  const fromDomain = carrier.register_from_domain || realm;
  const from = `sip:${fromUser}@${fromDomain}`;
  const presentity = monitor.presentity_uri;
  const transport = 'udp';
  const scheme = 'sip';

  // Prefer outbound proxy; else registrar realm (same pattern as regbot)
  let proxy;
  if (carrier.outbound_sip_proxy) {
    proxy = `sip:${carrier.outbound_sip_proxy};transport=${transport}`;
  } else if (realm) {
    proxy = `sip:${realm};transport=${transport}`;
  }

  const contact = buildContact(srf, monitor.contact_user, transport, scheme);
  const eventPackage = cfg.event_package || 'dialog';
  const accept = eventPackage === 'presence'
    ? 'application/pidf+xml'
    : 'application/dialog-info+xml';

  // Fresh Call-ID on each (re)SUBSCRIBE so PBX issues a new dialog/NOTIFY
  // instead of retransmitting a digest we already stored (which left monitors stale).
  // Unsubscribe (Expires:0) keeps the existing Call-ID when present.
  const callId = (expires === 0 && monitor.sub_call_id)
    ? monitor.sub_call_id
    : `${monitor.blf_monitor_sid}-${Date.now()}`;
  await upsertSubscription({
    blf_monitor_sid: monitor.blf_monitor_sid,
    owner_node: srf.locals.regbot.myToken,
    sub_call_id: callId,
    sub_status: 'trying',
    last_error: null,
  });

  const reqOpts = {
    method: 'SUBSCRIBE',
    headers: {
      'Call-ID': callId,
      From: from,
      To: presentity,
      Contact: contact,
      Event: eventPackage,
      Accept: accept,
      Expires: expires,
      'User-Agent': useragent,
    },
    auth: {
      username: carrier.register_username,
      password: carrier.register_password,
    },
  };
  if (proxy) reqOpts.proxy = proxy;

  logger.info({
    presentity,
    contact_user: monitor.contact_user,
    expires,
    eventPackage,
  }, 'BLF sending SUBSCRIBE');

  const req = await srf.request(presentity, reqOpts);

  return new Promise((resolve) => {
    req.on('response', async(res) => {
      try {
        if (res.status >= 200 && res.status < 300) {
          let expiresSec = expires;
          if (res.has('Expires')) expiresSec = parseInt(res.get('Expires'), 10) || expires;
          const to = res.getParsedHeader('To');
          const fromHdr = res.getParsedHeader('From');
          const expiresAt = new Date(Date.now() + expiresSec * 1000);
          await upsertSubscription({
            blf_monitor_sid: monitor.blf_monitor_sid,
            owner_node: srf.locals.regbot.myToken,
            sub_call_id: callId,
            sub_local_tag: fromHdr?.params?.tag || null,
            sub_remote_tag: to?.params?.tag || null,
            sub_remote_target: presentity,
            sub_cseq: 1,
            sub_expires_at: expiresAt,
            sub_status: 'active',
            last_error: null,
          });
          resolve({ok: true, status: res.status});
        } else {
          await upsertSubscription({
            blf_monitor_sid: monitor.blf_monitor_sid,
            owner_node: srf.locals.regbot.myToken,
            sub_status: 'error',
            last_error: `SUBSCRIBE ${res.status}`,
          });
          logger.info({status: res.status, presentity}, 'BLF SUBSCRIBE failed');
          resolve({ok: false, status: res.status});
        }
      } catch (err) {
        logger.error({err}, 'BLF SUBSCRIBE response handling error');
        resolve({ok: false, error: err.message});
      }
    });
  });
}

async function sendUnsubscribe(logger, srf, {cfg, monitor}) {
  if (!monitor.sub_call_id || monitor.sub_status === 'none' || monitor.sub_status === 'terminated') {
    return;
  }
  try {
    await sendSubscribe(logger, srf, {cfg, monitor, expires: 0});
    await upsertSubscription({
      blf_monitor_sid: monitor.blf_monitor_sid,
      owner_node: srf.locals.regbot.myToken,
      sub_status: 'terminated',
      last_error: null,
    });
  } catch (err) {
    logger.info({err: err.message, monitor: monitor.blf_monitor_sid}, 'BLF unsubscribe error');
  }
}

async function reconcile(logger, srf) {
  if (!JAMBONES_BLF_ENABLED) return;
  if (!srf.locals.regbot?.active) return;
  if (running) return;
  running = true;
  try {
    const data = await apiCall('/v1/internal/Blf/reconcile');
    const configs = data?.configs || [];
    for (const cfg of configs) {
      if (!isCarrierRegistered(cfg.carrier)) {
        // unsubscribe any live dialogs we own for this config
        for (const monitor of cfg.monitors || []) {
          if (monitor.owner_node === srf.locals.regbot.myToken &&
              ['active', 'trying', 'pending'].includes(monitor.sub_status)) {
            await sendUnsubscribe(logger, srf, {cfg, monitor});
          }
        }
        continue;
      }

      for (const monitor of cfg.monitors || []) {
        if (!monitor.is_enabled) {
          if (monitor.owner_node === srf.locals.regbot.myToken &&
              ['active', 'trying', 'pending'].includes(monitor.sub_status)) {
            await sendUnsubscribe(logger, srf, {cfg, monitor});
          }
          continue;
        }
        if (needsRefresh(monitor, cfg.subscribe_expires, cfg.stale_seconds)) {
          await sendSubscribe(logger, srf, {
            cfg,
            monitor,
            expires: cfg.subscribe_expires || 3600,
          });
        }
      }
    }
  } catch (err) {
    logger.error({err: err.message}, 'BLF reconcile failure');
  } finally {
    running = false;
  }
}

async function stopAll(logger, srf) {
  stopped = true;
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  if (!JAMBONES_BLF_ENABLED) return;
  try {
    const data = await apiCall('/v1/internal/Blf/reconcile');
    for (const cfg of data?.configs || []) {
      for (const monitor of cfg.monitors || []) {
        if (monitor.owner_node === srf.locals.regbot?.myToken &&
            ['active', 'trying', 'pending'].includes(monitor.sub_status)) {
          await sendUnsubscribe(logger, srf, {cfg, monitor});
        }
      }
    }
  } catch (err) {
    logger.info({err: err.message}, 'BLF stopAll error');
  }
}

function start(logger, srf) {
  if (!JAMBONES_BLF_ENABLED) {
    logger.info('BLF reconciler not started (JAMBONES_BLF_ENABLED is off)');
    return;
  }
  if (!apiBase() || !JAMBONES_INTERNAL_TOKEN) {
    logger.error('BLF reconciler not started: missing API_INTERNAL_BASE_URL or JAMBONES_INTERNAL_TOKEN');
    return;
  }
  if (timer) {
    logger.debug('BLF reconciler already running');
    return;
  }
  stopped = false;
  logger.info({intervalSec: RECONCILE_MS / 1000}, 'starting BLF subscription reconciler');
  reconcile(logger, srf).catch((err) => logger.error({err}, 'initial BLF reconcile error'));
  timer = setInterval(() => {
    if (stopped) return;
    reconcile(logger, srf).catch((err) => logger.error({err}, 'BLF reconcile error'));
  }, RECONCILE_MS);
}

module.exports = {
  start,
  stopAll,
  reconcile,
};
