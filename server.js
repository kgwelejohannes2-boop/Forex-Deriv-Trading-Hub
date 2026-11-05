import express from 'express';
const app=express();
app.use(express.static('public'));
app.get('/api/health',(req,res)=>res.json({ok:true,service:'Trading Hub',time:new Date().toISOString()}));
app.listen(process.env.PORT||3000,'0.0.0.0',()=>console.log('Trading Hub ready'));