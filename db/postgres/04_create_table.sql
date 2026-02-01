CREATE table if not exists staging.stg_weather_observations(
	id bigserial primary key,
	city_id int not null,
	observation_ts timestamptz not null,
	temperature double precision check(temperature between -60 and 60),
	feels_like double precision check(feels_like between -60 and 60),
	humidity int check(humidity between 0 and 100),
	pressure int check(pressure > 800),
	wind_speed double precision,
	weather_main text,
	weather_description text,
	source text not null check(source in ('OWM')),
	ingested_at timestamptz not null,
	unique(city_id,observation_ts,source)
);