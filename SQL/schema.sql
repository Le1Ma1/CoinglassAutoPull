-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.bitfinex_margin_long_short_1d (
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  long_qty numeric,
  short_qty numeric,
  CONSTRAINT bitfinex_margin_long_short_1d_pkey PRIMARY KEY (symbol, ts_utc)
);
CREATE TABLE public.borrow_interest_rate_1d (
  exchange text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  interest_rate numeric,
  CONSTRAINT borrow_interest_rate_1d_pkey PRIMARY KEY (exchange, symbol, ts_utc)
);
CREATE TABLE public.coinbase_premium_index_1d (
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  premium_usd numeric,
  premium_rate numeric,
  CONSTRAINT coinbase_premium_index_1d_pkey PRIMARY KEY (ts_utc)
);
CREATE TABLE public.etf_bitcoin_flow_1d (
  date_utc date NOT NULL,
  total_flow_usd numeric,
  price_usd numeric,
  details jsonb,
  CONSTRAINT etf_bitcoin_flow_1d_pkey PRIMARY KEY (date_utc)
);
CREATE TABLE public.etf_bitcoin_net_assets_1d (
  date_utc date NOT NULL,
  net_assets_usd numeric,
  change_usd numeric,
  price_usd numeric,
  CONSTRAINT etf_bitcoin_net_assets_1d_pkey PRIMARY KEY (date_utc)
);
CREATE TABLE public.etf_premium_discount_1d (
  date_utc date NOT NULL,
  ticker text NOT NULL,
  nav_usd numeric,
  market_price_usd numeric,
  premium_discount numeric,
  CONSTRAINT etf_premium_discount_1d_pkey PRIMARY KEY (date_utc, ticker)
);
CREATE TABLE public.features_1d (
  asset text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  px_open double precision,
  px_high double precision,
  px_low double precision,
  px_close double precision,
  vol_usd double precision,
  oi_agg_close double precision,
  oi_stable_close double precision,
  oi_coinm_close double precision,
  funding_oiw_close double precision,
  funding_volw_close double precision,
  lsr_global double precision,
  lsr_top_accts double precision,
  lsr_top_pos double precision,
  ob_bids_usd double precision,
  ob_asks_usd double precision,
  ob_bids_qty double precision,
  ob_asks_qty double precision,
  ob_imb double precision,
  depth_ratio_q double precision,
  taker_buy_usd double precision,
  taker_sell_usd double precision,
  taker_imb double precision,
  liq_long_usd double precision,
  liq_short_usd double precision,
  liq_net double precision,
  etf_flow_usd double precision,
  etf_aum_usd double precision,
  etf_premdisc double precision,
  cpi_premium_rate double precision,
  bfx_long_qty double precision,
  bfx_short_qty double precision,
  borrow_ir double precision,
  puell double precision,
  s2f_next_halving integer,
  pi_ma110 double precision,
  pi_ma350x2 double precision,
  ret_1d double precision,
  roc_3 double precision,
  roc_5 double precision,
  roc_10 double precision,
  roc_20 double precision,
  roc_60 double precision,
  roc_120 double precision,
  roc_252 double precision,
  mom_3 double precision,
  mom_5 double precision,
  mom_10 double precision,
  mom_20 double precision,
  mom_60 double precision,
  mom_120 double precision,
  mom_252 double precision,
  sma_10 double precision,
  sma_20 double precision,
  sma_60 double precision,
  sma_120 double precision,
  sma_252 double precision,
  ema_12 double precision,
  ema_26 double precision,
  macd double precision,
  macd_signal_9 double precision,
  macd_hist double precision,
  bb_mid_20 double precision,
  bb_up_20 double precision,
  bb_dn_20 double precision,
  atr_14 double precision,
  rv_20 double precision,
  rv_60 double precision,
  rv_120 double precision,
  z_ret_20 double precision,
  z_ret_60 double precision,
  z_ret_120 double precision,
  d_oi_1 double precision,
  oi_roc_5 double precision,
  oi_roc_20 double precision,
  oi_roc_60 double precision,
  oi_z_60 double precision,
  d_funding_1 double precision,
  funding_ma_20 double precision,
  funding_ma_60 double precision,
  funding_z_60 double precision,
  lsr_ma20_global double precision,
  lsr_z60_global double precision,
  lsr_ma20_top_accts double precision,
  lsr_z60_top_accts double precision,
  lsr_ma20_top_pos double precision,
  lsr_z60_top_pos double precision,
  ob_imb_ma20 double precision,
  ob_imb_z60 double precision,
  depth_ratio_q_ma20 double precision,
  depth_ratio_q_z60 double precision,
  taker_imb_ma20 double precision,
  taker_imb_z60 double precision,
  taker_buy_ma20 double precision,
  taker_sell_ma20 double precision,
  taker_buy_z60 double precision,
  taker_sell_z60 double precision,
  liq_z60 double precision,
  etf_flow_z60 double precision,
  etf_aum_roc_5 double precision,
  etf_aum_roc_20 double precision,
  premdisc_ma20 double precision,
  premdisc_z60 double precision,
  cpi_ma20 double precision,
  cpi_z60 double precision,
  bfx_lr double precision,
  bfx_lr_d1 double precision,
  borrow_ir_ma20 double precision,
  puell_d1 double precision,
  s2f_d1 double precision,
  pi_ma110_d1 double precision,
  pi_ma350x2_d1 double precision,
  xsec_ret_rank double precision,
  xsec_mom_rank_20 double precision,
  xsec_vol_rank_60 double precision,
  rel_to_btc double precision,
  feature_ver integer NOT NULL DEFAULT 1,
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  ext_features jsonb NOT NULL DEFAULT '{}'::jsonb,
  date_utc date DEFAULT ((ts_utc AT TIME ZONE 'UTC'::text))::date,
  CONSTRAINT features_1d_pkey PRIMARY KEY (asset, ts_utc)
);
CREATE TABLE public.funding_oi_weight_1d (
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  CONSTRAINT funding_oi_weight_1d_pkey PRIMARY KEY (symbol, ts_utc)
);
CREATE TABLE public.funding_vol_weight_1d (
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  CONSTRAINT funding_vol_weight_1d_pkey PRIMARY KEY (symbol, ts_utc)
);
CREATE TABLE public.futures_candles_1d (
  exchange text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  volume_usd numeric,
  CONSTRAINT futures_candles_1d_pkey PRIMARY KEY (exchange, symbol, ts_utc)
);
CREATE TABLE public.futures_oi_agg_1d (
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  unit text NOT NULL DEFAULT 'usd'::text,
  CONSTRAINT futures_oi_agg_1d_pkey PRIMARY KEY (symbol, ts_utc, unit)
);
CREATE TABLE public.futures_oi_coin_margin_1d (
  exchange_list text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  CONSTRAINT futures_oi_coin_margin_1d_pkey PRIMARY KEY (exchange_list, symbol, ts_utc)
);
CREATE TABLE public.futures_oi_stablecoin_1d (
  exchange_list text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  CONSTRAINT futures_oi_stablecoin_1d_pkey PRIMARY KEY (exchange_list, symbol, ts_utc)
);
CREATE TABLE public.hk_etf_flow_1d (
  date_utc date NOT NULL,
  total_flow_usd numeric,
  price_usd numeric,
  details jsonb,
  CONSTRAINT hk_etf_flow_1d_pkey PRIMARY KEY (date_utc)
);
CREATE TABLE public.idx_pi_cycle_daily (
  date_utc date NOT NULL,
  price numeric,
  ma_110 numeric,
  ma_350_x2 numeric,
  CONSTRAINT idx_pi_cycle_daily_pkey PRIMARY KEY (date_utc)
);
CREATE TABLE public.idx_puell_multiple_daily (
  date_utc date NOT NULL,
  price numeric,
  puell_multiple numeric,
  CONSTRAINT idx_puell_multiple_daily_pkey PRIMARY KEY (date_utc)
);
CREATE TABLE public.idx_stock_to_flow_daily (
  date_utc date NOT NULL,
  price numeric,
  next_halving integer,
  CONSTRAINT idx_stock_to_flow_daily_pkey PRIMARY KEY (date_utc)
);
CREATE TABLE public.labels_1d (
  asset text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  y_ret_d1 double precision,
  y_dir_d1 smallint,
  y_vol_d1 smallint,
  date_utc date DEFAULT ((ts_utc AT TIME ZONE 'UTC'::text))::date,
  CONSTRAINT labels_1d_pkey PRIMARY KEY (asset, ts_utc)
);
CREATE TABLE public.liquidation_agg_1d (
  exchange_list text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  long_liq_usd numeric,
  short_liq_usd numeric,
  CONSTRAINT liquidation_agg_1d_pkey PRIMARY KEY (exchange_list, symbol, ts_utc)
);
CREATE TABLE public.long_short_global_1d (
  exchange text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  long_percent numeric,
  short_percent numeric,
  long_short_ratio numeric,
  CONSTRAINT long_short_global_1d_pkey PRIMARY KEY (exchange, symbol, ts_utc)
);
CREATE TABLE public.long_short_top_accounts_1d (
  exchange text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  long_percent numeric,
  short_percent numeric,
  long_short_ratio numeric,
  CONSTRAINT long_short_top_accounts_1d_pkey PRIMARY KEY (exchange, symbol, ts_utc)
);
CREATE TABLE public.long_short_top_positions_1d (
  exchange text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  long_percent numeric,
  short_percent numeric,
  long_short_ratio numeric,
  CONSTRAINT long_short_top_positions_1d_pkey PRIMARY KEY (exchange, symbol, ts_utc)
);
CREATE TABLE public.orderbook_agg_futures_1d (
  exchange_list text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  bids_usd numeric,
  bids_qty numeric,
  asks_usd numeric,
  asks_qty numeric,
  range_pct numeric NOT NULL DEFAULT 0,
  CONSTRAINT orderbook_agg_futures_1d_pkey PRIMARY KEY (exchange_list, symbol, ts_utc, range_pct)
);
CREATE TABLE public.spot_candles_1d (
  exchange text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  volume_usd numeric,
  CONSTRAINT spot_candles_1d_pkey PRIMARY KEY (exchange, symbol, ts_utc)
);
CREATE TABLE public.taker_vol_agg_futures_1d (
  exchange_list text NOT NULL,
  symbol text NOT NULL,
  ts_utc timestamp with time zone NOT NULL,
  date_utc date NOT NULL,
  buy_vol_usd numeric,
  sell_vol_usd numeric,
  CONSTRAINT taker_vol_agg_futures_1d_pkey PRIMARY KEY (exchange_list, symbol, ts_utc)
);