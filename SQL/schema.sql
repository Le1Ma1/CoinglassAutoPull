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