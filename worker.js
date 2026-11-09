
const CALLBACK_URL =
  "https://tiktok-callback.vosquezshoats.workers.dev/api/tiktok/oauth/callback";

const TIKTOK_AUTH_URL =
  "https://business-api.tiktok.com/portal/auth";

const TIKTOK_TOKEN_URL =
  "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/";

const encoder = new TextEncoder();

// Conservative budgets for Workers Free. All cursors and pending batches live
// in the existing state_json column; no D1 schema changes are required.
const SYNC_INTERVAL_MS = 6 * 60 * 60 * 1000;
const SYNC_STATE_ID = 1;
const SYNC_LEASE_MS = 4 * 60 * 1000;
const SYNC_MAX_EXTERNAL_REQUESTS = 12;
const SYNC_MAX_PAGES = 3;
const SYNC_MAX_WALL_MS = 18000;
const SYNC_RETRY_BASE_MS = 5 * 60 * 1000;

async function loadSyncRow(env) {
  return env.DB.prepare("SELECT state_json, updated_at FROM tiktok_sync_state WHERE id=?")
    .bind(SYNC_STATE_ID).first();
}
async function readSyncState(env) {
  const row = await loadSyncRow(env);
  if (!row?.state_json) return null;
  try { return JSON.parse(row.state_json); } catch { return null; }
}
async function saveSyncState(env, state, expectedUpdatedAt) {
  const now = new Date().toISOString();
  // A monotonic state version makes each persisted checkpoint distinct even
  // when several saves happen within the same millisecond.
  state.state_version = (Number(state.state_version) || 0) + 1;
  const json = JSON.stringify(state);
  if (expectedUpdatedAt == null) {
    const result = await env.DB.prepare(`INSERT OR IGNORE INTO tiktok_sync_state (id,state_json,updated_at) VALUES (?,?,?)`)
      .bind(SYNC_STATE_ID,json,now).run();
    return Boolean(result.meta?.changes);
  }
  // Compare the complete prior state payload. updated_at has millisecond
  // precision and is not a safe CAS token for rapid consecutive writes.
  const result = await env.DB.prepare(`UPDATE tiktok_sync_state SET state_json=?,updated_at=? WHERE id=? AND state_json=?`)
    .bind(json,now,SYNC_STATE_ID,expectedUpdatedAt).run();
  return Boolean(result.meta?.changes);
}
async function tiktokGet(url, token) {
  const response = await fetch(url,{headers:{"Access-Token":token},signal:AbortSignal.timeout(12000)});
  let result;
  try { result=await response.json(); } catch { throw new Error(`TikTok invalid JSON (HTTP ${response.status})`); }
  if (!response.ok || result.code!==0) {
    const e=new Error(`TikTok API HTTP ${response.status}, code ${result.code??"unknown"}: ${result.message??"unknown"}`);
    e.retryAfter=response.headers.get("Retry-After"); throw e;
  }
  return result;
}
function pageInfo(data) {
  const p=data?.page_info;
  if (!p || !Number.isInteger(Number(p.page)) || !Number.isInteger(Number(p.total_page)) || Number(p.total_page)<1) {
    throw new Error("TikTok page_info missing or invalid; refusing to advance cursor");
  }
  return {page:Number(p.page),total:Number(p.total_page),count:Number(p.total_number??0)};
}
function newSyncState(previous, now) {
  return {running:true,phase:"bc",started_at:new Date(now).toISOString(),cycle_due_at:new Date(now+SYNC_INTERVAL_MS).toISOString(),
    completed_at:previous?.completed_at??null,heartbeat_at:new Date(now).toISOString(),lease_until:null,
    bc_page:1,bc_ids:[],bc_total_pages:null,bc_index:0,account_page:1,account_total_pages:null,
    pending_assets:null,batch_index:0,processed_bcs:0,processed_accounts:0,new_accounts:0,updated_accounts:0,
    failed_pages:0,last_error:null,retry_at:null,spend_date:null,spend_bc_index:0,spend_batch_index:0,spend_page:1,spend_total_pages:null,spend_accum:{},spend_rows_written:0};
}
async function acquireSyncLease(env) {
  const row=await loadSyncRow(env); const now=Date.now(); let state=null;
  if(row?.state_json) { try { state=JSON.parse(row.state_json); } catch {} }
  if(state?.retry_at && Date.parse(state.retry_at)>now) return null;
  if(state?.running && Date.parse(state.lease_until||0)>now) return null;
  if(!state?.running) {
    if(state?.cycle_due_at && Date.parse(state.cycle_due_at)>now) return null;
    state=newSyncState(state,now);
  }
  state.running=true; state.lease_until=new Date(now+SYNC_LEASE_MS).toISOString(); state.heartbeat_at=new Date(now).toISOString();
  const ok=await saveSyncState(env,state,row?.state_json??null);
  const saved=ok?await loadSyncRow(env):null;
  return ok?{state,expectedStateJson:saved?.state_json}:null;
}

async function fetchScheduledSpendPage(token, advertiserIds, reportDate, page) {
  const reportParams = {
    advertiser_ids: advertiserIds,
    dimensions: ["advertiser_id", "stat_time_day"],
    metrics: ["spend"],
    start_date: reportDate,
    end_date: reportDate,
    page,
    page_size: 1000,
    query_lifetime: false,
    // spend_date is calculated as the previous UTC calendar day.
    query_in_utc0: true
  };
  const reportUrl = new URL("https://business-api.tiktok.com/open_api/v2.0/report/integrated/get/");
  for (const [key, value] of Object.entries(reportParams)) {
    reportUrl.searchParams.set(key, Array.isArray(value) ? JSON.stringify(value) : String(value));
  }
  let method = "GET";
  let response = await fetch(reportUrl.toString(), {
    method,
    headers: { "Access-Token": token },
    signal: AbortSignal.timeout(15000)
  });
  let attempts = 1;
  if (response.status === 405) {
    method = "POST";
    attempts++;
    response = await fetch("https://business-api.tiktok.com/open_api/v2.0/report/integrated/get/", {
      method,
      headers: { "Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify(reportParams),
      signal: AbortSignal.timeout(15000)
    });
  }
  const raw = await response.text();
  let result;
  try { result = JSON.parse(raw); }
  catch { throw new Error("TikTok scheduled Spend report returned non-JSON (HTTP " + response.status + ")"); }
  if (!response.ok || result.code !== 0) {
    const error = new Error("TikTok scheduled Spend report failed (HTTP " + response.status +
      ", code " + (result.code ?? "unknown") + ", message " + String(result.message || "unknown").slice(0, 200) + ")");
    error.retryAfter = response.headers.get("Retry-After");
    throw error;
  }
  const data = result.data;
  if (data?.extra_info?.search_ads_throttle) {
    throw new Error("TikTok scheduled Spend report was truncated by the search limit: " +
      String(data.extra_info.search_ads_throttle).slice(0, 200));
  }
  const info = data?.page_info;
  if (!Array.isArray(data?.list) || !info ||
      !Number.isInteger(Number(info.page)) || Number(info.page) !== page ||
      !Number.isInteger(Number(info.total_page)) || Number(info.total_page) < 0) {
    throw new Error("TikTok scheduled Spend report returned invalid page data");
  }
  return { rows: data.list, totalPages: Number(info.total_page), attempts };
}

async function runScheduledSync(env) {
  let lease=await acquireSyncLease(env); if(!lease) return;
  let {state,expectedStateJson}=lease;
  const started=Date.now(), deadline=started+SYNC_MAX_WALL_MS; let requests=0,pages=0;
  const persist=async()=>{state.heartbeat_at=new Date().toISOString();state.lease_until=new Date(Date.now()+SYNC_LEASE_MS).toISOString();
    if(!expectedStateJson || !await saveSyncState(env,state,expectedStateJson)) throw new Error("Sync lease lost to another Cron invocation");
    expectedStateJson=(await loadSyncRow(env))?.state_json;};
  try {
    const token=await getAccessToken(env);
    while(requests<SYNC_MAX_EXTERNAL_REQUESTS && pages<SYNC_MAX_PAGES && Date.now()<deadline) {
      if(state.phase==="bc") {
        const u=new URL("https://business-api.tiktok.com/open_api/v1.3/bc/get/");u.searchParams.set("page",String(state.bc_page));u.searchParams.set("page_size","50");
        const r=await tiktokGet(u,token);requests++;const list=r.data?.list;
        if(!Array.isArray(list)) throw new Error("BC response list missing; cursor retained");
        const pi=pageInfo(r.data);
        for(const x of list){const b=x.bc_info??x;if(!b.bc_id||!b.name)continue;const id=String(b.bc_id);
          await env.DB.prepare(`INSERT INTO business_centers (business_center_id,name,updated_at) VALUES (?,?,?) ON CONFLICT(business_center_id) DO UPDATE SET name=excluded.name,updated_at=excluded.updated_at`).bind(id,b.name,new Date().toISOString()).run();
          if(!state.bc_ids.includes(id))state.bc_ids.push(id);}
        state.bc_total_pages=pi.total;if(pi.page<pi.total)state.bc_page++;else{state.phase="accounts";state.bc_index=0;}pages++;
      } else if(state.phase==="accounts") {
        if(state.bc_index>=state.bc_ids.length){state.phase="spend";state.spend_date=new Date(Date.now()-86400000).toISOString().slice(0,10);state.spend_bc_index=0;state.spend_batch_index=0;state.spend_page=1;state.spend_total_pages=null;state.spend_accum={};await persist();continue;}
        const bc=state.bc_ids[state.bc_index];state.current_bc=bc;state.current_page=state.account_page;
        if(!state.pending_assets){const u=new URL("https://business-api.tiktok.com/open_api/v1.3/bc/asset/get/");u.searchParams.set("bc_id",bc);u.searchParams.set("asset_type","ADVERTISER");u.searchParams.set("page",String(state.account_page));u.searchParams.set("page_size","50");
          const r=await tiktokGet(u,token);requests++;if(!Array.isArray(r.data?.list))throw new Error("Advertiser asset list missing; cursor retained");const pi=pageInfo(r.data);state.pending_assets=r.data.list;state.account_total_pages=pi.total;state.batch_index=0;pages++;await persist();}
        const assets=state.pending_assets, batches=[];for(let i=0;i<assets.length;i+=20)batches.push(assets.slice(i,i+20));
        if(state.batch_index<batches.length){if(requests>=SYNC_MAX_EXTERNAL_REQUESTS)break;
          const batch=batches[state.batch_index],ids=batch.map(a=>String(a.asset_id)).filter(Boolean);if(!ids.length)throw new Error("Asset batch contains no advertiser IDs");
          const u=new URL("https://business-api.tiktok.com/open_api/v1.3/advertiser/info/");u.searchParams.set("advertiser_ids",JSON.stringify(ids));const ir=await tiktokGet(u,token);requests++;
          if(!Array.isArray(ir.data?.list))throw new Error("Advertiser info list missing; batch retained");const found=new Map(ir.data.list.map(a=>[String(a.advertiser_id),a]));
          for(const asset of batch){const id=String(asset.asset_id),info=found.get(id);if(!info?.name||!info?.currency)throw new Error(`Details missing for advertiser ${id}; batch retained`);
            const old=await env.DB.prepare("SELECT 1 AS yes FROM advertiser_accounts WHERE business_center_id=? AND advertiser_id=?").bind(bc,id).first();
            await env.DB.prepare(`INSERT INTO advertiser_accounts (business_center_id,advertiser_id,advertiser_name,currency,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(business_center_id,advertiser_id) DO UPDATE SET advertiser_name=excluded.advertiser_name,currency=excluded.currency,updated_at=excluded.updated_at`).bind(bc,id,info.name,info.currency,new Date().toISOString()).run();
            if(old)state.updated_accounts++;else state.new_accounts++;state.processed_accounts++;}
          state.batch_index++;await persist();continue;
        }
        state.pending_assets=null;state.batch_index=0;
        if(state.account_page<state.account_total_pages)state.account_page++;else{state.bc_index++;state.processed_bcs++;state.account_page=1;state.account_total_pages=null;}pages++;

      } else if(state.phase==="spend") {
        if(state.spend_bc_index>=state.bc_ids.length){
          state.running=false;state.phase="complete";state.completed_at=new Date().toISOString();
          state.cycle_due_at=new Date(Date.now()+SYNC_INTERVAL_MS).toISOString();state.lease_until=null;state.retry_at=null;
          await persist();return;
        }
        const bc=state.bc_ids[state.spend_bc_index];
        const accountResult=await env.DB.prepare(
          "SELECT advertiser_id, currency FROM advertiser_accounts WHERE business_center_id=? ORDER BY advertiser_id"
        ).bind(bc).all();
        const accounts=(accountResult.results||[]).map(row=>({
          advertiser_id:String(row.advertiser_id),currency:String(row.currency||"UNKNOWN")
        }));
        const batches=[];for(let i=0;i<accounts.length;i+=20)batches.push(accounts.slice(i,i+20));
        if(state.spend_batch_index>=batches.length){
          state.spend_bc_index++;state.spend_batch_index=0;state.spend_page=1;state.spend_total_pages=null;state.spend_accum={};
          await persist();continue;
        }
        const batch=batches[state.spend_batch_index];
        // Reserve room for a GET-to-POST fallback if TikTok returns HTTP 405.
        if(requests>=SYNC_MAX_EXTERNAL_REQUESTS-1 || pages>=SYNC_MAX_PAGES) break;
        const pageResult=await fetchScheduledSpendPage(token,batch.map(a=>a.advertiser_id),state.spend_date,state.spend_page);
        requests+=pageResult.attempts;pages++;
        const allowed=new Set(batch.map(a=>a.advertiser_id));
        const pageAccum={};
        for(const item of pageResult.rows){
          const advertiserId=String(item.dimensions?.advertiser_id||"");
          const rowDate=item.dimensions?.stat_time_day;
          const spend=Number(item.metrics?.spend);
          if(!allowed.has(advertiserId)||rowDate!==state.spend_date||!Number.isFinite(spend))
            throw new Error("Scheduled Spend row did not match requested advertiser/date");
          const fallbackCurrency=batch.find(a=>a.advertiser_id===advertiserId)?.currency;
          const currency=typeof item.metrics?.currency==="string"&&item.metrics.currency&&item.metrics.currency!=="-"
            ?item.metrics.currency:fallbackCurrency;
          if(typeof currency!=="string"||!currency.trim()||currency==="UNKNOWN")
            throw new Error(`Currency missing for advertiser ${advertiserId}; refusing to write ambiguous Spend`);
          const prior=pageAccum[advertiserId];
          pageAccum[advertiserId]={
            spend:Math.round(((prior?.spend||0)+spend+Number.EPSILON)*100)/100,
            currency
          };
        }
        for(const [advertiserId,row] of Object.entries(pageAccum)){
          const prior=state.spend_accum[advertiserId];
          state.spend_accum[advertiserId]={
            spend:Math.round(((prior?.spend||0)+row.spend+Number.EPSILON)*100)/100,
            currency:row.currency
          };
        }
        state.spend_total_pages=pageResult.totalPages;
        if(state.spend_page<state.spend_total_pages){
          state.spend_page++;await persist();continue;
        }
        const timestamp=new Date().toISOString();
        const rowsToWrite=batch.map(account=>{
          const report=state.spend_accum[account.advertiser_id];
          const currency=report?.currency||account.currency;
          if(typeof currency!=="string"||!currency.trim()||currency==="UNKNOWN")
            throw new Error(`Currency missing for advertiser ${account.advertiser_id}; refusing to write ambiguous Spend`);
          return {account,currency,spend:String(report?.spend??0)};
        });
        for(const row of rowsToWrite){
          await env.DB.prepare(
            "INSERT INTO daily_spend (business_center_id,advertiser_id,report_date,currency,spend,updated_at) VALUES (?,?,?,?,?,?) ON CONFLICT(business_center_id,advertiser_id,report_date,currency) DO UPDATE SET spend=excluded.spend,updated_at=excluded.updated_at"
          ).bind(bc,row.account.advertiser_id,state.spend_date,row.currency,row.spend,timestamp).run();
          state.spend_rows_written++;
        }
        state.spend_batch_index++;state.spend_page=1;state.spend_total_pages=null;state.spend_accum={};
        await persist();
      } else throw new Error(`Unknown sync phase: ${state.phase}`);
      await persist();
    }
  } catch(error) {
    state.failed_pages++;state.last_error=String(error.message||"Sync failed").slice(0,500);
    const retryMs=error.retryAfter?Math.max(60000,Number(error.retryAfter)*1000):Math.min(60*60*1000,SYNC_RETRY_BASE_MS*Math.pow(2,Math.min(state.failed_pages-1,4)));
    state.retry_at=new Date(Date.now()+retryMs).toISOString();state.running=true;state.lease_until=null;
    state.heartbeat_at=new Date().toISOString();
    if(expectedStateJson) await saveSyncState(env,state,expectedStateJson);
    console.error("Scheduled TikTok sync paused for retry:",state.last_error);
  }
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}

function hexToBytes(hex) {
  if (
    typeof hex !== "string" ||
    !/^(?:[0-9a-fA-F]{2})+$/.test(hex)
  ) {
    throw new Error("Invalid hexadecimal data");
  }

  const bytes = new Uint8Array(hex.length / 2);

  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(
      hex.slice(i * 2, i * 2 + 2),
      16
    );
  }

  return bytes;
}

function bytesToHex(bytes) {
  return Array.from(bytes)
    .map(byte => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function sha256(value) {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    encoder.encode(value)
  );

  return bytesToHex(new Uint8Array(digest));
}

async function encryptToken(token, secret) {
  const keyBytes = hexToBytes(secret);

  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "AES-GCM" },
    false,
    ["encrypt"]
  );

  const nonce = crypto.getRandomValues(new Uint8Array(12));

  const encrypted = await crypto.subtle.encrypt(
    { name: "AES-GCM", iv: nonce },
    key,
    encoder.encode(token)
  );

  return {
    ciphertext: bytesToHex(new Uint8Array(encrypted)),
    nonce: bytesToHex(nonce)
  };
}
async function decryptToken(ciphertext, nonceHex, secret) {
  const keyBytes = hexToBytes(secret);

  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "AES-GCM" },
    false,
    ["decrypt"]
  );

  const decrypted = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: hexToBytes(nonceHex)
    },
    key,
    hexToBytes(ciphertext)
  );

  return new TextDecoder().decode(decrypted);
}
async function getAccessToken(env) {
  const credential = await env.DB.prepare(`
    SELECT encrypted_access_token, token_nonce
    FROM oauth_credentials
    WHERE app_id = ?
    ORDER BY id DESC
    LIMIT 1
  `).bind(env.TIKTOK_APP_ID).first();

  if (!credential) {
    throw new Error("No TikTok authorization record found.");
  }

  return await decryptToken(
    credential.encrypted_access_token,
    credential.token_nonce,
    env.TOKEN_ENCRYPTION_KEY
  );
}
async function startAuthorization(env) {
  const stateBytes = crypto.getRandomValues(
    new Uint8Array(32)
  );

  const state = bytesToHex(stateBytes);
  const stateHash = await sha256(state);

  const now = new Date();
  const expires = new Date(now.getTime() + 10 * 60 * 1000);

  await env.DB.prepare(`
    INSERT INTO oauth_sessions
    (state_hash, created_at, expires_at, used_at)
    VALUES (?, ?, ?, NULL)
  `).bind(
    stateHash,
    now.toISOString(),
    expires.toISOString()
  ).run();

  const authUrl = new URL(TIKTOK_AUTH_URL);

  authUrl.searchParams.set("app_id", env.TIKTOK_APP_ID);
  authUrl.searchParams.set("state", state);
  authUrl.searchParams.set("redirect_uri", CALLBACK_URL);

  return Response.redirect(authUrl.toString(), 302);
}

async function handleCallback(request, env) {
  const url = new URL(request.url);

  const authCode =
    url.searchParams.get("auth_code") ||
    url.searchParams.get("code");

  const state = url.searchParams.get("state");
  const error = url.searchParams.get("error");

  if (error) {
    return new Response(
      "TikTok authorization was not completed.",
      { status: 400 }
    );
  }

  if (!authCode || !state) {
    return new Response(
      "Missing authorization code or state.",
      { status: 400 }
    );
  }

  const stateHash = await sha256(state);

  const now = new Date().toISOString();

  const stateResult = await env.DB.prepare(`
    UPDATE oauth_sessions
    SET used_at = ?
    WHERE state_hash = ?
      AND used_at IS NULL
      AND expires_at > ?
  `).bind(
    now,
    stateHash,
    now
  ).run();

  if (!stateResult.meta || stateResult.meta.changes !== 1) {
    return new Response(
      "Invalid, expired, or already used authorization state.",
      { status: 403 }
    );
  }

  const tokenResponse = await fetch(TIKTOK_TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      app_id: env.TIKTOK_APP_ID,
      secret: env.TIKTOK_APP_SECRET,
      auth_code: authCode
    })
  });

  const result = await tokenResponse.json();

  if (
    !tokenResponse.ok ||
    result.code !== 0 ||
    !result.data ||
    !result.data.access_token
  ) {
    console.error(
      "TikTok token exchange failed:",
      result.code || "Unknown error"
    );

    return new Response(
      "TikTok token exchange failed. Please start authorization again.",
      { status: 502 }
    );
  }

  const encrypted = await encryptToken(
    result.data.access_token,
    env.TOKEN_ENCRYPTION_KEY
  );

  const timestamp = new Date().toISOString();

  await env.DB.prepare(`
    INSERT INTO oauth_credentials (
      app_id,
      encrypted_access_token,
      token_nonce,
      encryption_version,
      created_at,
      updated_at
    )
    VALUES (?, ?, ?, 1, ?, ?)
  `).bind(
    env.TIKTOK_APP_ID,
    encrypted.ciphertext,
    encrypted.nonce,
    timestamp,
    timestamp
  ).run();

  return new Response(
    "TikTok authorization successful. Encrypted token saved.",
    {
      status: 200,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-store"
      }
    }
  );
}

export default {
  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(runScheduledSync(env));
  },
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/api/tiktok/sync-status") {
      const providedKey = request.headers.get("X-Dashboard-Key");
      if (!env.DASHBOARD_API_KEY || providedKey !== env.DASHBOARD_API_KEY) return new Response("Unauthorized", { status: 401 });
      try {
        const state = await readSyncState(env);
        return jsonResponse({
          success: true, running: Boolean(state?.running), started_at: state?.started_at ?? null,
          completed_at: state?.completed_at ?? null, current_bc: state?.current_bc ?? null,
          current_page: state?.current_page ?? null, processed_bcs: state?.processed_bcs ?? 0,
          processed_accounts: state?.processed_accounts ?? 0, new_accounts: state?.new_accounts ?? 0,
          updated_accounts: state?.updated_accounts ?? 0, phase: state?.phase ?? null,
          spend_date: state?.spend_date ?? null, spend_rows_written: state?.spend_rows_written ?? 0,
          failed_pages: state?.failed_pages ?? 0, last_error: state?.last_error ?? null
        });
      } catch (error) {
        console.error("Sync status read failed:", error.message);
        return jsonResponse({ success: false, error: "Sync status unavailable." }, 500);
      }
    }

    if (url.pathname === "/") {
      return new Response(
        "TikTok Marketing API Worker is running."
      );
    }

    if (url.pathname === "/api/tiktok/oauth/start") {
      if (!env.TIKTOK_APP_ID || !env.TIKTOK_APP_SECRET) {
        return new Response(
          "TikTok app configuration is incomplete.",
          { status: 500 }
        );
      }

      return startAuthorization(env);
    }

    if (url.pathname === "/api/tiktok/oauth/callback") {
      if (
        !env.TIKTOK_APP_ID ||
        !env.TIKTOK_APP_SECRET ||
        !env.TOKEN_ENCRYPTION_KEY ||
        !env.DB
      ) {
        return new Response(
          "Server configuration is incomplete.",
          { status: 500 }
        );
      }

      try {
        return await handleCallback(request, env);
      } catch (error) {
        console.error(
          "OAuth callback processing failed:",
          error.message
        );

        return new Response(
          "Internal server error. Please check Worker logs.",
          { status: 500 }
        );
      }
    }
    if (url.pathname === "/api/tiktok/test-token") {
      const providedKey = request.headers.get("X-Dashboard-Key");

      if (
        !env.DASHBOARD_API_KEY ||
        providedKey !== env.DASHBOARD_API_KEY
      ) {
        return new Response("Unauthorized", { status: 401 });
      }

      try {
        const token = await getAccessToken(env);

        return Response.json({
          success: true,
          token_decryption: "success",
          token_present: Boolean(token)
        });
      } catch (error) {
        console.error("Token test failed:", error.message);

        return Response.json({
          success: false,
          error: "Token decryption failed."
        }, { status: 500 });
      }
    }
    if (url.pathname === "/api/tiktok/business-centers") {
      const providedKey = request.headers.get("X-Dashboard-Key");
      if (!env.DASHBOARD_API_KEY || providedKey !== env.DASHBOARD_API_KEY) {
        return new Response("Unauthorized", { status: 401 });
      }
      try {
        // Use the complete, already-synchronized D1 history instead of the
        // first live TikTok page. This is read-only and preserves old BC links.
        const result = await env.DB.prepare(
          "SELECT ids.business_center_id, COALESCE(MAX(bc.name), 'Business Center ' || ids.business_center_id) AS name FROM (SELECT business_center_id FROM business_centers UNION SELECT business_center_id FROM advertiser_accounts) ids LEFT JOIN business_centers bc ON bc.business_center_id = ids.business_center_id GROUP BY ids.business_center_id ORDER BY name COLLATE NOCASE, ids.business_center_id"
        ).all();
        const list = (result.results || []).map(row => ({
          bc_info: { bc_id: String(row.business_center_id), name: row.name }
        }));
        return jsonResponse({ success: true, count: list.length, source: "d1", data: { list } });
      } catch (error) {
        console.error("Business Center history query failed:", error.message);
        return jsonResponse({ success: false, error: "Business Center history query failed." }, 500);
      }
    }
      if (url.pathname === "/api/tiktok/sync-business-centers") {
  const providedKey = request.headers.get("X-Dashboard-Key");

  if (
    !env.DASHBOARD_API_KEY ||
    providedKey !== env.DASHBOARD_API_KEY
  ) {
    return new Response("Unauthorized", { status: 401 });
  }

  try {
    const token = await getAccessToken(env);

    const apiResponse = await fetch(
      "https://business-api.tiktok.com/open_api/v1.3/bc/get/?page=1&page_size=10",
      {
        method: "GET",
        headers: {
          "Access-Token": token
        }
      }
    );

    const result = await apiResponse.json();

    if (!apiResponse.ok || result.code !== 0) {
      return Response.json({
        success: false,
        error: "TikTok Business Center query failed.",
        tiktok_code: result.code ?? null,
        message: result.message ?? null
      }, { status: 502 });
    }

    const list = result.data?.list ?? [];
    let savedCount = 0;

    for (const item of list) {
      const bc = item.bc_info;

      if (!bc?.bc_id || !bc?.name) {
        continue;
      }

      await env.DB.prepare(`
        INSERT INTO business_centers (
          business_center_id,
          name,
          updated_at
        )
        VALUES (?, ?, ?)
        ON CONFLICT(business_center_id)
        DO UPDATE SET
          name = excluded.name,
          updated_at = excluded.updated_at
      `).bind(
        bc.bc_id,
        bc.name,
        new Date().toISOString()
      ).run();

      savedCount++;
    }

    return Response.json({
      success: true,
      fetched: list.length,
      saved: savedCount
    });
  } catch (error) {
    console.error("Business Center sync failed:", error.message);

    return Response.json({
      success: false,
      error: "Business Center sync failed."
    }, { status: 500 });
  }
} 
      if (url.pathname === "/api/tiktok/daily-spend") {
      const providedKey = request.headers.get("X-Dashboard-Key");

      if (
        !env.DASHBOARD_API_KEY ||
        providedKey !== env.DASHBOARD_API_KEY
      ) {
        return new Response("Unauthorized", { status: 401 });
      }

      const startDate = url.searchParams.get("start_date");
      const endDate = url.searchParams.get("end_date");
      const businessCenterId = url.searchParams.get("bc_id");

      if (
        !startDate ||
        !endDate ||
        !/^\d{4}-\d{2}-\d{2}$/.test(startDate) ||
        !/^\d{4}-\d{2}-\d{2}$/.test(endDate) ||
        startDate > endDate
      ) {
        return Response.json({
          success: false,
          error: "Please provide valid start_date and end_date."
        }, { status: 400 });
      }

      try {
        let query = `
          SELECT
            business_center_id,
            advertiser_id,
            report_date,
            currency,
            spend
          FROM daily_spend
          WHERE report_date >= ?
            AND report_date <= ?
        `;

        const params = [startDate, endDate];

        if (businessCenterId) {
          query += " AND business_center_id = ?";
          params.push(businessCenterId);
        }

        query += `
          ORDER BY
            report_date,
            business_center_id,
            advertiser_id,
            currency
        `;

        const result = await env.DB.prepare(query)
          .bind(...params)
          .all();

        return Response.json({
          success: true,
          count: result.results.length,
          data: result.results
        });
      } catch (error) {
        console.error("Daily spend query failed:", error.message);

        return Response.json({
          success: false,
          error: "Daily spend query failed."
        }, { status: 500 });
      }
    }
  
if (url.pathname === "/api/tiktok/daily-report") {
      const providedKey = request.headers.get("X-Dashboard-Key");
      if (!env.DASHBOARD_API_KEY || providedKey !== env.DASHBOARD_API_KEY) {
        return new Response("Unauthorized", { status: 401 });
      }

      const reportDate = url.searchParams.get("date");
      const businessCenterId = url.searchParams.get("bc_id");
      const parsedDate = reportDate ? new Date(reportDate + "T00:00:00.000Z") : null;
      if (
        !reportDate ||
        !businessCenterId ||
        reportDate.length !== 10 ||
        reportDate[4] !== "-" ||
        reportDate[7] !== "-" ||
        !Number.isFinite(parsedDate?.getTime()) ||
        parsedDate.toISOString().slice(0, 10) !== reportDate
      ) {
        return Response.json({
          success: false,
          error: "Please provide a valid date in YYYY-MM-DD format."
        }, { status: 400 });
      }

      try {
        // Load BC/account associations and names from existing D1 history.
        // The same advertiser is intentionally kept under every associated BC.
        const associationResult = await env.DB.prepare(
          "SELECT ids.business_center_id, COALESCE(MAX(bc.name), 'Business Center ' || ids.business_center_id) AS business_center_name, aa.advertiser_id, MAX(aa.advertiser_name) AS advertiser_name, MAX(aa.currency) AS currency FROM (SELECT business_center_id FROM business_centers UNION SELECT business_center_id FROM advertiser_accounts) ids LEFT JOIN business_centers bc ON bc.business_center_id = ids.business_center_id LEFT JOIN advertiser_accounts aa ON aa.business_center_id = ids.business_center_id WHERE ids.business_center_id = ? GROUP BY ids.business_center_id, aa.advertiser_id ORDER BY business_center_name COLLATE NOCASE, ids.business_center_id, advertiser_name COLLATE NOCASE, aa.advertiser_id"
        ).bind(businessCenterId).all();

        const centers = new Map();
        for (const row of associationResult.results || []) {
          const bcId = String(row.business_center_id);
          if (!centers.has(bcId)) {
            centers.set(bcId, {
              business_center_id: bcId,
              business_center_name: row.business_center_name,
              currency_totals: {},
              accounts: [],
              accountMap: new Map()
            });
          }
          if (!row.advertiser_id) continue;
          const center = centers.get(bcId);
          const account = {
            advertiser_id: String(row.advertiser_id),
            advertiser_name: row.advertiser_name || ("Advertiser " + row.advertiser_id),
            currency: String(row.currency || "UNKNOWN"),
            spend: 0
          };
          center.accountMap.set(account.advertiser_id, account);
          center.accounts.push(account);
        }

        const token = await getAccessToken(env);
        const jobs = [];
        for (const center of centers.values()) {
          const accounts = [...center.accountMap.values()];
          for (let i = 0; i < accounts.length; i += 20) {
            jobs.push({ center, accounts: accounts.slice(i, i + 20) });
          }
        }
        if (jobs.length > 100) {
          return Response.json({
            success: false,
            error: "This report exceeds the safe request limit. Split the BC list before retrying."
          }, { status: 413 });
        }

        let nextJob = 0;
        let requestFailure = null;
        const fetchBatch = async job => {
          const ids = job.accounts.map(account => account.advertiser_id);
          let page = 1;
          let totalPages = 1;
          do {
            // TikTok's v2.0 synchronous-report reference labels this POST,
            // while its own curl example and the official SDK use GET.
            // Try GET first and retry POST only when the server rejects the method.
            const reportUrl = new URL(
              "https://business-api.tiktok.com/open_api/v2.0/report/integrated/get/"
            );
            const reportParams = {
              advertiser_ids: ids,
              dimensions: ["advertiser_id", "stat_time_day"],
              metrics: ["spend"],
              start_date: reportDate,
              end_date: reportDate,
              page,
              page_size: 1000,
              query_lifetime: false,
              query_in_utc0: false
            };
            for (const [key, value] of Object.entries(reportParams)) {
              reportUrl.searchParams.set(
                key,
                Array.isArray(value) ? JSON.stringify(value) : String(value)
              );
            }
            let requestMethod = "GET";
            let response = await fetch(reportUrl.toString(), {
              method: requestMethod,
              headers: { "Access-Token": token },
              signal: AbortSignal.timeout(15000)
            });
            if (response.status === 405) {
              requestMethod = "POST";
              response = await fetch(
                "https://business-api.tiktok.com/open_api/v2.0/report/integrated/get/",
                {
                  method: requestMethod,
                  headers: {
                    "Access-Token": token,
                    "Content-Type": "application/json"
                  },
                  body: JSON.stringify(reportParams),
                  signal: AbortSignal.timeout(15000)
                }
              );
            }
            const rawBody = await response.text();
            let result;
            try {
              result = JSON.parse(rawBody);
            } catch {
              const preview = rawBody.slice(0, 240).split(token).join("[redacted]");
              throw new Error(
                "TikTok report " + requestMethod + " /open_api/v2.0/report/integrated/get/ returned non-JSON (HTTP " +
                response.status + ", content-type " +
                (response.headers.get("Content-Type") || "unknown") +
                ", body " + JSON.stringify(preview) + ")"
              );
            }
            if (!response.ok || result.code !== 0) {
              throw new Error(
                "TikTok report " + requestMethod + " /open_api/v2.0/report/integrated/get/ failed (HTTP " +
                response.status + ", code " + (result.code ?? "unknown") +
                ", message " + String(result.message || "unknown").slice(0, 200) + ")"
              );
            }

            const data = result.data;
            // TikTok may truncate multi-advertiser results at its search limit.
            // Never present a partial response as a complete daily total.
            if (data?.extra_info?.search_ads_throttle) {
              throw new Error(
                "TikTok report was truncated by the search limit: " +
                String(data.extra_info.search_ads_throttle).slice(0, 240)
              );
            }
            const info = data?.page_info;
            if (
              !Array.isArray(data?.list) ||
              !info ||
              info.page == null ||
              info.total_page == null ||
              !Number.isInteger(Number(info.page)) ||
              Number(info.page) !== page ||
              !Number.isInteger(Number(info.total_page)) ||
              Number(info.total_page) < 0
            ) {
              throw new Error(
                "TikTok report page_info/list invalid for " + requestMethod + " /open_api/v2.0/report/integrated/get/ (page " +
                page + ")"
              );
            }
            totalPages = Number(info.total_page);
            if (totalPages === 0 && data.list.length !== 0) {
              throw new Error("TikTok report returned rows while total_page was zero");
            }

            for (const item of data.list) {
              const advertiserId = String(item.dimensions?.advertiser_id || "");
              const rowDate = item.dimensions?.stat_time_day;
              const spendValue = item.metrics?.spend;
              const spend = Number(spendValue);
              const account = job.center.accountMap.get(advertiserId);
              if (!account || !ids.includes(advertiserId) || rowDate !== reportDate || !Number.isFinite(spend)) {
                throw new Error("TikTok report row did not match the requested advertiser/date");
              }
              // For multi-advertiser reports TikTok adds currency to metrics;
              // prefer that live value over potentially stale D1 account metadata.
              const reportCurrency = item.metrics?.currency;
              if (typeof reportCurrency === "string" && reportCurrency && reportCurrency !== "-") {
                account.currency = reportCurrency;
              }
              account.spend = Math.round((account.spend + spend + Number.EPSILON) * 100) / 100;
            }
            page++;
          } while (page <= totalPages);
        };

        const workers = Array.from({ length: Math.min(4, jobs.length) }, async () => {
          while (nextJob < jobs.length && !requestFailure) {
            const job = jobs[nextJob++];
            try {
              await fetchBatch(job);
            } catch (error) {
              requestFailure = error;
            }
          }
        });
        await Promise.all(workers);
        if (requestFailure) throw requestFailure;

        // Save the verified live report into historical D1 storage. The
        // composite primary key makes repeated reads safe and idempotent.
        for (const center of centers.values()) {
          for (const account of center.accounts) {
            if(typeof account.currency!=="string"||!account.currency.trim()||account.currency==="UNKNOWN") {
              throw new Error(`Currency missing for advertiser ${account.advertiser_id}; refusing to persist ambiguous Spend`);
            }
          }
        }
        const persistedAt = new Date().toISOString();
        for (const center of centers.values()) {
          for (const account of center.accounts) {
            await env.DB.prepare(
              "INSERT INTO daily_spend (business_center_id,advertiser_id,report_date,currency,spend,updated_at) VALUES (?,?,?,?,?,?) ON CONFLICT(business_center_id,advertiser_id,report_date,currency) DO UPDATE SET spend=excluded.spend,updated_at=excluded.updated_at"
            ).bind(center.business_center_id,account.advertiser_id,reportDate,account.currency,String(account.spend),persistedAt).run();
          }
        }

        const businessCenters = [...centers.values()].map(center => {
          const currencyTotals = {};
          for (const account of center.accounts) {
            currencyTotals[account.currency] =
              Math.round(((currencyTotals[account.currency] || 0) + account.spend + Number.EPSILON) * 100) / 100;
          }
          center.accounts.sort((a, b) => b.spend - a.spend || a.advertiser_name.localeCompare(b.advertiser_name));
          return {
            business_center_id: center.business_center_id,
            business_center_name: center.business_center_name,
            currency_totals: currencyTotals,
            accounts: center.accounts
          };
        });

        return Response.json({
          success: true,
          date: reportDate,
          business_center_count: businessCenters.length,
          account_rows: businessCenters.reduce((count, bc) => count + bc.accounts.length, 0),
          data: { business_centers: businessCenters }
        });
      } catch (error) {
        console.error("Daily BC report failed:", error.message);
        return Response.json({
          success: false,
          error: String(error.message || "Daily BC report failed.").slice(0, 500)
        }, { status: 502 });
      }
    }

    if (url.pathname === "/api/tiktok/sync-business-center-accounts") {
  const providedKey = request.headers.get("X-Dashboard-Key");

  if (
    !env.DASHBOARD_API_KEY ||
    providedKey !== env.DASHBOARD_API_KEY
  ) {
    return new Response("Unauthorized", { status: 401 });
  }

  const businessCenterId = url.searchParams.get("bc_id");
  const page = Number(url.searchParams.get("page") || "1");

  if (!businessCenterId || !Number.isInteger(page) || page < 1) {
    return Response.json({
      success: false,
      error: "Please provide a valid bc_id and page."
    }, { status: 400 });
  }

  try {
    const token = await getAccessToken(env);

    // 1. 获取当前 BC 的一页广告账户
    const assetUrl = new URL(
  "https://business-api.tiktok.com/open_api/v1.3/bc/asset/get/"
);

    assetUrl.searchParams.set("bc_id", businessCenterId);
    assetUrl.searchParams.set("asset_type", "ADVERTISER");
    assetUrl.searchParams.set("page", String(page));
    assetUrl.searchParams.set("page_size", "50");

    const assetResponse = await fetch(assetUrl.toString(), {
      method: "GET",
      headers: {
        "Access-Token": token
      }
    });

    const assetResult = await assetResponse.json();

    if (!assetResponse.ok || assetResult.code !== 0) {
      return Response.json({
        success: false,
        step: "asset_query",
        message: assetResult.message ?? null,
        tiktok_code: assetResult.code ?? null
      }, { status: 502 });
    }

    const assets = assetResult.data?.list ?? [];
    const pageInfo = assetResult.data?.page_info ?? {};
    const advertiserIds = assets
      .map(item => item.asset_id)
      .filter(Boolean);

    if (advertiserIds.length === 0) {
      return Response.json({
        success: true,
        bc_id: businessCenterId,
        page,
        total_pages: pageInfo.total_page ?? 0,
        fetched: 0,
        saved: 0
      });
    }

    // 2. 每 20 个账户查询一次详细信息
    const advertiserInfoMap = new Map();

    for (let i = 0; i < advertiserIds.length; i += 20) {
      const batchIds = advertiserIds.slice(i, i + 20);

     const infoUrl = new URL(
  "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
);
      infoUrl.searchParams.set(
        "advertiser_ids",
        JSON.stringify(batchIds)
      );

      const infoResponse = await fetch(infoUrl.toString(), {
        method: "GET",
        headers: {
          "Access-Token": token
        }
      });

      const infoResult = await infoResponse.json();

      if (!infoResponse.ok || infoResult.code !== 0) {
        return Response.json({
          success: false,
          step: "advertiser_info",
          message: infoResult.message ?? null,
          tiktok_code: infoResult.code ?? null
        }, { status: 502 });
      }

      for (const advertiser of infoResult.data?.list ?? []) {
        advertiserInfoMap.set(
          String(advertiser.advertiser_id),
          advertiser
        );
      }
    }

    // 3. 保存账户到 D1
    const statements = [];
    const missingDetails = [];

    for (const asset of assets) {
      const advertiserId = String(asset.asset_id);
      const info = advertiserInfoMap.get(advertiserId);

      if (!info?.currency) {
        missingDetails.push(advertiserId);
        continue;
      }

      const advertiserName = info.name || asset.asset_name;

      if (!advertiserName) {
        missingDetails.push(advertiserId);
        continue;
      }

      statements.push(
        env.DB.prepare(`
          INSERT INTO advertiser_accounts (
            business_center_id,
            advertiser_id,
            advertiser_name,
            currency,
            updated_at
          )
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(business_center_id, advertiser_id)
          DO UPDATE SET
            advertiser_name = excluded.advertiser_name,
            currency = excluded.currency,
            updated_at = excluded.updated_at
        `).bind(
          businessCenterId,
          advertiserId,
          advertiserName,
          info.currency,
          new Date().toISOString()
        )
      );
    }

    if (statements.length > 0) {
      await env.DB.batch(statements);
    }

    return Response.json({
      success: true,
      bc_id: businessCenterId,
      page,
      total_pages: pageInfo.total_page ?? null,
      total_accounts: pageInfo.total_number ?? null,
      fetched: assets.length,
      saved: statements.length,
      missing_details: missingDetails
    });

  } catch (error) {
    console.error(
      "Business Center account sync failed:",
      error.message
    );

    return Response.json({
      success: false,
      error: "Business Center account sync failed."
    }, { status: 500 });
  }
}
 
if (url.pathname === "/api/tiktok/business-center-accounts") {
  const providedKey = request.headers.get("X-Dashboard-Key");
  if (!env.DASHBOARD_API_KEY || providedKey !== env.DASHBOARD_API_KEY) {
    return new Response("Unauthorized", { status: 401 });
  }
  const businessCenterId = url.searchParams.get("bc_id");
  if (!businessCenterId) {
    return jsonResponse({ success: false, error: "Please provide bc_id." }, 400);
  }
  try {
    // Return every stored association, including historical relationships.
    // The dashboard must not silently truncate at TikTok's first 50 assets.
    const result = await env.DB.prepare(
      "SELECT advertiser_id, advertiser_name, currency FROM advertiser_accounts WHERE business_center_id = ? ORDER BY advertiser_name COLLATE NOCASE, advertiser_id"
    ).bind(businessCenterId).all();
    const list = (result.results || []).map(row => ({
      asset_id: String(row.advertiser_id),
      asset_name: row.advertiser_name,
      currency: row.currency
    }));
    return jsonResponse({
      success: true, count: list.length, source: "d1",
      data: { list, page_info: { page: 1, total_page: 1, total_number: list.length } }
    });
  } catch (error) {
    console.error("Advertiser account history query failed:", error.message);
    return jsonResponse({ success: false, error: "Advertiser account history query failed." }, 500);
  }
}

if (url.pathname === "/api/tiktok/advertiser-info") {
    const advertiserId = url.searchParams.get("advertiser_id");
  const advertiserIdsParam = url.searchParams.get("advertiser_ids");

  const advertiserIds = advertiserIdsParam
    ? advertiserIdsParam.split(",").map(id => id.trim()).filter(Boolean)
    : advertiserId
      ? [advertiserId]
      : [];

  if (advertiserIds.length === 0) {
    return Response.json({
      success: false,
      error: "Please provide advertiser_id or advertiser_ids."
    }, { status: 400 });
  }

  if (advertiserIds.length > 20) {
    return Response.json({
      success: false,
      error: "A maximum of 20 advertiser IDs is allowed per request."
    }, { status: 400 });
  }

  try {
    const token = await getAccessToken(env);

    const apiUrl = new URL(
      "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
    );

  apiUrl.searchParams.set(
  "advertiser_ids",
  JSON.stringify(advertiserIds)
 );

    const apiResponse = await fetch(apiUrl.toString(), {
      method: "GET",
      headers: {
        "Access-Token": token
      }
    });

    const result = await apiResponse.json();

    return Response.json({
      success: apiResponse.ok && result.code === 0,
      http_status: apiResponse.status,
      tiktok_code: result.code ?? null,
      message: result.message ?? null,
      data: result.data ?? null
    });
  } catch (error) {
    console.error("Advertiser info query failed:", error.message);

    return Response.json({
      success: false,
      error: "Advertiser info query failed."
    }, { status: 500 });
  }
}

    return new Response("Not Found", { status: 404 });
  }
};
