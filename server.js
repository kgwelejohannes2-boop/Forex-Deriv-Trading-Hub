import express from 'express';
import WebSocket from 'ws';

const app = express();
app.use(express.static('public'));
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

const forexPairs=['EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF','NZD/USD','USD/ZAR','EUR/GBP','GBP/JPY'];
const forexCache=new Map();
app.get('/api/forex/status',(_req,res)=>res.json({priceFeedConfigured:Boolean(process.env.TWELVE_DATA_API_KEY),fundamentalsFeedConfigured:false,pairs:forexPairs}));
app.get('/api/forex/candles',async(req,res)=>{
 const pair=String(req.query.pair||'EUR/USD'),tf=String(req.query.interval||'15min');
 if(!forexPairs.includes(pair)||!['5min','15min','1h'].includes(tf))return res.status(400).json({error:'Unsupported pair or timeframe'});
 const key=process.env.TWELVE_DATA_API_KEY;
 if(!key)return res.status(503).json({error:'Twelve Data API key not configured. Add TWELVE_DATA_API_KEY to Render environment.'});
 const cacheKey=pair+':'+tf,old=forexCache.get(cacheKey);
 if(old&&Date.now()-old.time<60000)return res.json(old.data);
 try{
  const url=new URL('https://api.twelvedata.com/time_series');
  url.searchParams.set('symbol',pair);url.searchParams.set('interval',tf);url.searchParams.set('outputsize','120');url.searchParams.set('apikey',key);
  const response=await fetch(url,{signal:AbortSignal.timeout(12000)});
  const data=await response.json();
  if(!response.ok||data.status==='error'||!Array.isArray(data.values))return res.status(502).json({error:data.message||'Forex data provider returned no candles'});
  const candles=data.values.map(x=>({time:x.datetime,open:Number(x.open),high:Number(x.high),low:Number(x.low),close:Number(x.close)})).reverse();
  const result={pair,interval:tf,candles,provider:'Twelve Data',providerTimeZone:data.meta?.exchange_timezone||'UTC'};
  forexCache.set(cacheKey,{time:Date.now(),data:result});res.json(result);
 }catch(e){res.status(502).json({error:'Forex price feed unavailable: '+e.message});}
});

app.listen(process.env.PORT||3000,'0.0.0.0',()=>console.log('Trading Hub ready'));