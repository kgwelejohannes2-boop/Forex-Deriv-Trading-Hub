import express from 'express';
import WebSocket from 'ws';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const app = express();
const here = path.dirname(fileURLToPath(import.meta.url));
// Serve the page from public/index.html when it exists. Otherwise look for the page next to server.js or in a
// differently-cased "Public" folder, and accept names like "index (1).html". If nothing is found, say what the
// server can see instead of a bare "Cannot GET /".
app.use(express.static('public'));
function listDir(d){try{return fs.readdirSync(d,{withFileTypes:true});}catch{return [];}}
function findPage(){
  const dirs=[here,...listDir(here).filter(e=>e.isDirectory()&&e.name.toLowerCase()==='public').map(e=>path.join(here,e.name))];
  for(const d of dirs){
    const files=listDir(d).filter(e=>e.isFile()).map(e=>e.name);
    const hit=files.find(f=>f.toLowerCase()==='index.html')||files.find(f=>/^index.*\.html?$/i.test(f));
    if(hit)return path.join(d,hit);
  }
  return null;
}
app.get('/', (_req, res) => {
  const page=findPage();
  if(page)return res.sendFile(page);
  const top=listDir(here).filter(e=>e.name!=='node_modules'&&!e.name.startsWith('.')).map(e=>(e.isDirectory()?'[folder] ':'[file]   ')+e.name);
  res.status(404).type('text/plain').send('Trading Hub server is running, but no index.html page was found.\n\nWhat the server can see at the top level:\n'+top.join('\n')+'\n');
});
app.get('/api/health', (_req,res) => res.json({ok:true,service:'Trading Hub',time:new Date().toISOString()}));

const endpoint='wss://api.derivws.com/trading/v1/options/ws/public';
let catalog=null, catalogAt=0;
function fetchCatalog(){
 return new Promise((resolve,reject)=>{
  const ws=new WebSocket(endpoint,{handshakeTimeout:12000});
  const timer=setTimeout(()=>{ws.terminate();reject(new Error('Deriv symbol lookup timed out'));},16000);
  const finish=(err,data)=>{clearTimeout(timer);ws.close();err?reject(err):resolve(data);};
  ws.once('open',()=>ws.send(JSON.stringify({active_symbols:'brief'})));
  ws.on('message',raw=>{try{const d=JSON.parse(raw.toString());if(d.error)finish(new Error(d.error.message));else if(d.active_symbols)finish(null,d.active_symbols);}catch(e){finish(e);}});
  ws.once('error',e=>{clearTimeout(timer);reject(e);});
 });
}
app.get('/api/deriv/symbols',async(_req,res)=>{
 try{
  if(!catalog||Date.now()-catalogAt>3600000){const all=await fetchCatalog();catalog=all.filter(x=>x && typeof (x.symbol||x.underlying_symbol)==='string' && (String(x.market||'').includes('synthetic') || String(x.market_display_name||'').toLowerCase().includes('synthetic')));catalogAt=Date.now();}
  res.json({symbols:catalog.map(x=>({symbol:x.symbol||x.underlying_symbol,name:x.display_name||x.symbol||x.underlying_symbol})),count:catalog.length});
 }catch(e){res.status(502).json({error:e.message});}
});

// Bounded batch scanner: one upstream connection, six symbols per request.
app.get('/api/deriv/scan',async(req,res)=>{
 const requested=String(req.query.symbols||'').split(',').filter(Boolean).slice(0,6);
 if(!requested.length||requested.some(x=>!/^[A-Za-z0-9_]{2,40}$/.test(x)))return res.status(400).json({error:'Invalid symbol batch'});
 try{
  const known=catalog||await fetchCatalog();
  const valid=new Set(known.map(x=>x.symbol||x.underlying_symbol));
  if(requested.some(x=>!valid.has(x)))return res.status(400).json({error:'Unknown symbol'});
  const results=await new Promise((resolve,reject)=>{
   const ws=new WebSocket(endpoint,{handshakeTimeout:12000}),received=new Map(),errors=[];
   let settled=false;
   const finish=()=>{if(settled)return;settled=true;clearTimeout(timer);ws.close();resolve({results:[...received.values()],errors});};
   const timer=setTimeout(finish,22000);
   ws.on('open',()=>{requested.forEach((symbol,i)=>{for(const tf of [300,900])ws.send(JSON.stringify({ticks_history:symbol,count:65,end:'latest',style:'candles',granularity:tf,req_id:1000+i*2+(tf===900?1:0)}));});});
   ws.on('message',raw=>{try{
    const d=JSON.parse(raw.toString()),id=Number(d.req_id),index=Math.floor((id-1000)/2),symbol=requested[index],tf=(id-1000)%2?900:300;
    if(!symbol)return;
    if(d.error){errors.push({symbol,timeframe:tf,message:d.error.message});}
    else if(d.candles){const entry=received.get(symbol)||{symbol};entry[tf]=d.candles;received.set(symbol,entry);}
    if(received.size===requested.length&&requested.every(x=>received.get(x)?.[300]&&received.get(x)?.[900]))finish();
   }catch(e){errors.push({message:e.message});}});
   ws.on('error',e=>{if(!settled){settled=true;clearTimeout(timer);reject(e);}});
   ws.on('close',()=>{if(!settled)finish();});
  });
  res.json(results);
 }catch(e){res.status(502).json({error:e.message});}
});
app.get('/api/deriv/stream', (req,res) => {
  const symbol = String(req.query.symbol || 'R_10');
  if (!/^[A-Za-z0-9_]{2,40}$/.test(symbol)) return res.status(400).json({error:'Invalid symbol'});
  res.setHeader('Content-Type','text/event-stream');
  res.setHeader('Cache-Control','no-cache, no-transform');
  res.setHeader('Connection','keep-alive');
  res.flushHeaders();
  const send = (type,data) => { if(!res.writableEnded) res.write('event: '+type+'\ndata: '+JSON.stringify(data)+'\n\n'); };
  send('status',{message:'Connecting to Deriv through server…'});
  const socket = new WebSocket('wss://api.derivws.com/trading/v1/options/ws/public', {handshakeTimeout:12000});
  let opened=false;
  const timeout=setTimeout(()=>{send('problem',{message:'Deriv upstream timed out'});socket.terminate();},18000);
  const heartbeat=setInterval(()=>{if(!res.writableEnded)res.write(': ping\n\n');},15000);
  socket.on('open',()=>{opened=true;clearTimeout(timeout);send('status',{message:'Connected; subscribing to '+symbol});socket.send(JSON.stringify({ticks_history:symbol,adjust_start_time:1,count:100,end:'latest',style:'candles',granularity:300,req_id:300}));socket.send(JSON.stringify({ticks_history:symbol,adjust_start_time:1,count:100,end:'latest',style:'candles',granularity:900,req_id:900}));socket.send(JSON.stringify({ticks:symbol,subscribe:1}));});
  socket.on('message',raw=>{
    try{
      const data=JSON.parse(raw.toString());
      if(data.error){send('problem',{message:data.error.message,code:data.error.code});console.error('Deriv API:',data.error.code,data.error.message);socket.close();}
      else if(data.candles)send('candles',{timeframe:data.echo_req?.granularity || data.req_id,candles:data.candles});
      else if(data.tick)send('tick',{symbol,quote:data.tick.quote,epoch:data.tick.epoch});
    }catch(e){console.error('Deriv parse:',e.message);}
  });
  socket.on('error',e=>{console.error('Deriv upstream error:',e.message);send('problem',{message:'Server could not connect to Deriv: '+e.message});});
  socket.on('close',(code)=>{clearTimeout(timeout);send('status',{message:'Deriv connection closed (code '+code+')'});if(!res.writableEnded)res.end();});
  req.on('close',()=>{clearTimeout(timeout);clearInterval(heartbeat);if(socket.readyState===WebSocket.OPEN||socket.readyState===WebSocket.CONNECTING)socket.terminate();});
});

const forexPairs=['EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF','NZD/USD','USD/ZAR','EUR/GBP','GBP/JPY','XAU/USD','BTC/USD'];
const forexCache=new Map();

// ---- Deriv forex price feed (primary). Twelve Data stays only as an optional fallback. ----
const derivForex={'EUR/USD':'frxEURUSD','GBP/USD':'frxGBPUSD','USD/JPY':'frxUSDJPY','AUD/USD':'frxAUDUSD','USD/CAD':'frxUSDCAD','USD/CHF':'frxUSDCHF','NZD/USD':'frxNZDUSD','USD/ZAR':'frxUSDZAR','EUR/GBP':'frxEURGBP','GBP/JPY':'frxGBPJPY','XAU/USD':'frxXAUUSD','BTC/USD':'cryBTCUSD'};
const tfSeconds={'5min':300,'15min':900,'1h':3600};
const derivTtl={'5min':30000,'15min':60000,'1h':120000};
const twelveTtl={'5min':240000,'15min':840000,'1h':3540000};

// One short-lived connection per call; resolves a Map of req_id -> response.
function derivCall(requests,timeoutMs=20000){
 return new Promise((resolve,reject)=>{
  const ws=new WebSocket(endpoint,{handshakeTimeout:12000}),got=new Map();
  let done=false;
  const finish=err=>{if(done)return;done=true;clearTimeout(timer);try{ws.terminate();}catch{}err?reject(err):resolve(got);};
  const timer=setTimeout(()=>finish(new Error('Deriv request timed out')),timeoutMs);
  ws.once('open',()=>requests.forEach(r=>ws.send(JSON.stringify(r))));
  ws.on('message',raw=>{try{
   const d=JSON.parse(raw.toString()),id=Number(d.req_id??d.echo_req?.req_id);
   if(!Number.isFinite(id))return;
   got.set(id,d);
   if(requests.every(r=>got.has(r.req_id)))finish();
  }catch(e){finish(e);}});
  ws.once('error',e=>finish(e));
  ws.once('close',()=>finish(new Error('Deriv connection closed early')));
 });
}

let derivSymbols=null,derivSymbolsAt=0;
async function derivSymbolSet(){
 if(derivSymbols&&Date.now()-derivSymbolsAt<3600000)return derivSymbols;
 const d=(await derivCall([{active_symbols:'brief',req_id:1}])).get(1);
 if(d?.error)throw new Error(d.error.message);
 derivSymbols=new Set((d?.active_symbols||[]).map(x=>x.symbol||x.underlying_symbol).filter(Boolean));
 derivSymbolsAt=Date.now();
 return derivSymbols;
}

const utcText=epoch=>new Date(Number(epoch)*1000).toISOString().slice(0,19).replace('T',' ');
const derivInflight=new Map();
// Fetches H1, M15 and M5 for one pair in a single connection. Output matches the old Twelve Data shape
// (oldest first, time as 'YYYY-MM-DD HH:MM:SS' UTC) so the page code does not change.
function derivForexSets(pair){
 if(derivInflight.has(pair))return derivInflight.get(pair);
 const symbol=derivForex[pair],tfs=Object.keys(tfSeconds);
 const job=(async()=>{
  const got=await derivCall(tfs.map((tf,i)=>({ticks_history:symbol,adjust_start_time:1,count:120,end:'latest',style:'candles',granularity:tfSeconds[tf],req_id:10+i})));
  const out={};
  tfs.forEach((tf,i)=>{
   const d=got.get(10+i);
   if(!d)throw new Error('No Deriv response for '+symbol+' '+tf);
   if(d.error)throw new Error(d.error.message||'Deriv error for '+symbol);
   const candles=(Array.isArray(d.candles)?d.candles:[]).map(x=>({time:utcText(x.epoch??x.open_time),open:Number(x.open),high:Number(x.high),low:Number(x.low),close:Number(x.close)})).filter(x=>[x.open,x.high,x.low,x.close].every(Number.isFinite)&&!x.time.startsWith('Invalid'));
   if(!candles.length)throw new Error('Deriv returned no candles for '+symbol+' '+tf);
   out[tf]=candles;
  });
  return out;
 })().finally(()=>derivInflight.delete(pair));
 derivInflight.set(pair,job);
 return job;
}

// Public scheduled economic events. Calendar does NOT establish directional bias.
let calendarCache={at:0,events:[]};
app.get('/api/forex/calendar',async(req,res)=>{
 const pair=String(req.query.pair||'EUR/USD');
 if(!forexPairs.includes(pair))return res.status(400).json({error:'Unsupported market'});
 const currencies=pair==='XAU/USD'||pair==='BTC/USD'?['USD']:pair.split('/').filter(x=>x!=='ZAR');
 try{
  if(!calendarCache.at||Date.now()-calendarCache.at>900000){
   const url='https://showmytrades.com/api/public/calendar/v1?impact=high&days=3';
   const upstream=await fetch(url,{signal:AbortSignal.timeout(12000)});
   if(!upstream.ok)throw new Error('Calendar HTTP '+upstream.status);
   const payload=await upstream.json();
   if(!Array.isArray(payload.events))throw new Error('Unexpected calendar format');
   calendarCache={at:Date.now(),events:payload.events};
  }
  const now=Date.now();
  const events=calendarCache.events.filter(x=>currencies.includes(x.currency)&&Number.isFinite(Number(x.ts))).map(x=>({
   currency:x.currency,title:String(x.title||'Economic event'),impact:x.impact,time:new Date(Number(x.ts)*1000).toISOString(),ts:Number(x.ts),status:String(x.status||'scheduled')
  })).sort((a,b)=>a.ts-b.ts);
  const blocked=events.some(x=>Math.abs(x.ts*1000-now)<=30*60000);
  res.json({pair,calendarAvailable:true,source:'ShowMyTrades',fetchedAt:new Date(calendarCache.at).toISOString(),newsBlocked:blocked,events:events.slice(0,25),directionalBias:'UNAVAILABLE',confirmedSignalAllowed:false});
 }catch(e){
  res.status(503).json({pair,calendarAvailable:false,newsBlocked:true,directionalBias:'UNAVAILABLE',confirmedSignalAllowed:false,error:e.message});
 }
});
app.get('/api/forex/status',async(_req,res)=>{
 let available=null,unavailable=[];
 try{const set=await derivSymbolSet();if(set.size){available=forexPairs.filter(p=>set.has(derivForex[p]));unavailable=forexPairs.filter(p=>!set.has(derivForex[p]));}}catch{}
 res.json({priceFeedConfigured:true,provider:'Deriv',fallbackConfigured:Boolean(process.env.TWELVE_DATA_API_KEY),fundamentalsFeedConfigured:false,pairs:forexPairs,derivAvailable:available,derivUnavailable:unavailable});
});
app.get('/api/forex/candles',async(req,res)=>{
 const pair=String(req.query.pair||'EUR/USD'),tf=String(req.query.interval||'15min');
 if(!forexPairs.includes(pair)||!tfSeconds[tf])return res.status(400).json({error:'Unsupported pair or timeframe'});
 const cacheKey=pair+':'+tf,old=forexCache.get(cacheKey);
 if(old&&Date.now()-old.time<old.ttl)return res.json({...old.data,cacheHit:true});

 // 1) Deriv (primary)
 let derivError;
 try{
  const sets=await derivForexSets(pair);
  for(const k of Object.keys(sets)){
   forexCache.set(pair+':'+k,{time:Date.now(),ttl:derivTtl[k],data:{pair,interval:k,candles:sets[k],provider:'Deriv',providerTimeZone:'UTC',cacheHit:false}});
  }
  return res.json(forexCache.get(cacheKey).data);
 }catch(e){derivError=e;console.error('Deriv forex',pair,tf,e.message);}

 // 2) Twelve Data fallback, only if a key is configured
 const key=process.env.TWELVE_DATA_API_KEY;
 if(!key)return res.status(502).json({error:'Deriv forex feed unavailable for '+pair+': '+derivError.message,pair,interval:tf});
 try{
  const url=new URL('https://api.twelvedata.com/time_series');
  url.searchParams.set('symbol',pair);url.searchParams.set('interval',tf);url.searchParams.set('outputsize','120');url.searchParams.set('timezone','UTC');url.searchParams.set('apikey',key);
  const response=await fetch(url,{signal:AbortSignal.timeout(12000)});
  const data=await response.json();
  if(!response.ok||data.status==='error'||!Array.isArray(data.values)){
   const message=String(data.message||'Forex data provider returned no candles');
   const limited=response.status===429||data.code===429||/limit|credit|quota|too many requests/i.test(message);
   if(limited)res.setHeader('Retry-After','60');
   return res.status(limited?429:502).json({error:'Deriv: '+derivError.message+' | Twelve Data: '+message,code:data.code||response.status,rateLimited:limited,pair,interval:tf});
  }
  const candles=data.values.map(x=>({time:x.datetime,open:Number(x.open),high:Number(x.high),low:Number(x.low),close:Number(x.close)})).reverse();
  const result={pair,interval:tf,candles,provider:'Twelve Data (fallback)',providerTimeZone:'UTC',cacheHit:false};
  forexCache.set(cacheKey,{time:Date.now(),ttl:twelveTtl[tf],data:result});res.json(result);
 }catch(e){res.status(502).json({error:'Forex price feed unavailable: '+e.message});}
});

app.listen(process.env.PORT||3000,'0.0.0.0',()=>console.log('Trading Hub ready'));