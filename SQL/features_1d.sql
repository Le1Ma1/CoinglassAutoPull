-- features_1d
create table if not exists public.features_1d (
  asset          text        not null,
  ts_utc         timestamptz not null,
  date_utc       date        generated always as ((ts_utc at time zone 'utc')::date) stored,

  px_open double precision, px_high double precision, px_low double precision, px_close double precision, vol_usd double precision,

  ret_1d double precision,
  roc_3 double precision,  roc_5 double precision,  roc_10 double precision, roc_20 double precision,
  roc_60 double precision, roc_120 double precision, roc_252 double precision,
  mom_3 double precision,  mom_5 double precision,  mom_10 double precision, mom_20 double precision,
  mom_60 double precision, mom_120 double precision, mom_252 double precision,

  sma_10 double precision, sma_20 double precision, sma_60 double precision, sma_120 double precision, sma_252 double precision,
  ema_12 double precision, ema_26 double precision,
  macd double precision, macd_signal_9 double precision, macd_hist double precision,
  bb_mid_20 double precision, bb_up_20 double precision, bb_dn_20 double precision,
  atr_14 double precision,

  rv_20 double precision, rv_60 double precision, rv_120 double precision,
  z_ret_20 double precision, z_ret_60 double precision, z_ret_120 double precision,

  oi_agg_close double precision, oi_stable_close double precision, oi_coinm_close double precision,
  d_oi_1 double precision, oi_roc_5 double precision, oi_roc_20 double precision, oi_roc_60 double precision, oi_z_60 double precision,

  funding_oiw_close double precision, funding_volw_close double precision,
  d_funding_1 double precision, funding_ma_20 double precision, funding_ma_60 double precision, funding_z_60 double precision,

  lsr_global double precision, lsr_top_accts double precision, lsr_top_pos double precision,
  lsr_ma20_global double precision, lsr_ma20_top_accts double precision, lsr_ma20_top_pos double precision,
  lsr_z60_global double precision,  lsr_z60_top_accts double precision,  lsr_z60_top_pos double precision,

  ob_bids_usd double precision, ob_asks_usd double precision, ob_bids_qty double precision, ob_asks_qty double precision,
  ob_imb double precision, depth_ratio_q double precision,
  ob_imb_ma20 double precision, ob_imb_z60 double precision,
  depth_ratio_q_ma20 double precision, depth_ratio_q_z60 double precision,

  taker_buy_usd double precision, taker_sell_usd double precision, taker_imb double precision,
  taker_imb_ma20 double precision, taker_imb_z60 double precision,
  taker_buy_ma20 double precision, taker_sell_ma20 double precision,
  taker_buy_z60 double precision, taker_sell_z60 double precision,

  liq_long_usd double precision, liq_short_usd double precision, liq_net double precision, liq_z60 double precision,

  etf_flow_usd double precision, etf_aum_usd double precision, etf_premdisc double precision,
  etf_flow_z60 double precision, etf_aum_roc_5 double precision, etf_aum_roc_20 double precision,
  premdisc_ma20 double precision, premdisc_z60 double precision,

  cpi_premium_rate double precision, cpi_ma20 double precision, cpi_z60 double precision,

  bfx_long_qty double precision, bfx_short_qty double precision, bfx_lr double precision, bfx_lr_d1 double precision,

  borrow_ir double precision, borrow_ir_ma20 double precision,
  puell double precision, puell_d1 double precision,
  s2f double precision,   s2f_d1 double precision,

  pi_ma110 double precision, pi_ma350x2 double precision, pi_diff double precision, pi_diff_d1 double precision,

  xsec_ret_rank double precision, xsec_mom_rank_20 double precision, xsec_vol_rank_60 double precision,
  rel_to_btc double precision,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint pk_features_1d primary key (asset, ts_utc),
  constraint chk_rank_bounds check (
    (xsec_ret_rank    is null or (xsec_ret_rank    >= 0 and xsec_ret_rank    <= 1)) and
    (xsec_mom_rank_20 is null or (xsec_mom_rank_20 >= 0 and xsec_mom_rank_20 <= 1)) and
    (xsec_vol_rank_60 is null or (xsec_vol_rank_60 >= 0 and xsec_vol_rank_60 <= 1))
  )
);

create index if not exists idx_features_1d_ts    on public.features_1d using brin(ts_utc);
create index if not exists idx_features_1d_date  on public.features_1d(date_utc);
create index if not exists idx_features_1d_asset on public.features_1d(asset);

create or replace function public.tg_set_updated_at() returns trigger language plpgsql as $$
begin new.updated_at = now(); return new; end $$;
drop trigger if exists trg_features_1d_updated on public.features_1d;
create trigger trg_features_1d_updated before update on public.features_1d
for each row execute function public.tg_set_updated_at();

-- 可選 labels_1d
create table if not exists public.labels_1d (
  asset text not null,
  ts_utc timestamptz not null,
  date_utc date generated always as ((ts_utc at time zone 'utc')::date) stored,
  y_ret_d1 double precision,
  y_dir_d1 smallint,
  y_vol_d1 double precision,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint pk_labels_1d primary key (asset, ts_utc),
  constraint chk_y_dir check (y_dir_d1 in (-1,0,1))
);
create index if not exists idx_labels_1d_ts on public.labels_1d using brin(ts_utc);
create index if not exists idx_labels_1d_date on public.labels_1d(date_utc);
