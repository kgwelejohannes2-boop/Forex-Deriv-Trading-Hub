# Forex + Deriv Trading Hub V1

Mobile-friendly starter dashboard with a Node/Express server, health endpoint and public Deriv tick subscription. Forex and strategy signals are clearly marked as not yet implemented.

## Local run
`npm install && npm start` then open `http://localhost:3000`.

## Deploy on Render
In Render, choose **New > Blueprint**, connect the repository, and apply `render.yaml`. Or create a Web Service with build `npm install` and start `npm start`, selecting the free plan if offered.

## Next steps
Implement secure Forex API backend; retrieve Deriv candle history; implement breakout, retest, ATR and multi-timeframe confirmations; add tested signals and history. Deriv API availability and WebSocket policies may change. Never put private API keys in browser code.