create index if not exists idx_stg_weather_obs_ts
on staging.stg_weather_observations(observation_ts);