import express from 'express';
import WebSocket from 'ws';

const app = express();
app.use(express.static('public'));
app.get('/api/health', (_req,res) => res.json({ok:true,service:'Trading Hub',time:new Date().toISOString()}));

const allowed = new Set(['R_10','R_25','R_50','R_75','R_100','BOOM1000','CRASH1000','JD10','stpRNG']);
app.get('/api/deriv/stream', (req,res) => {
  const symbol = String(req.query.symbol || 'R_10');
  if (!allowed.has(symbol)) return res.status(400).json({error:'Unsupported symbol'});
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
app.listen(process.env.PORT||3000,'0.0.0.0',()=>console.log('Trading Hub ready'));