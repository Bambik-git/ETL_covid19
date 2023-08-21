CREATE SCHEMA IF NOT EXISTS Data_Mart;

--создание таблицы для записи json
CREATE TABLE public.covid19_json (
	id serial4 NOT NULL,
	date_of_data date NULL,
	iso_country varchar(5) NULL,
	json_data jsonb NULL,
	CONSTRAINT covid19_pkey PRIMARY KEY (id)
);
CREATE INDEX covid19_json_date_idx ON public.covid19_json USING btree (date_of_data);


--Создание промежуточной таблицы
create table public.covid19_table
(
	day_of_data date NULL,
	active int4 NULL,
	deaths int4 NULL,
	confirmed int4 NULL,
	recovered int4 NULL,
	active_diff int4 NULL,
	deaths_diff int4 NULL,
	confirmed_diff int4 null,
	recovered_diff int4 null,
	fatality_rate float8 NULL,
	region_name varchar(60) null, 
	country_name varchar(20) null,
	lat float8 null,
	long float8 null,
	date_key int4 NULL,
	region_key int2 NULL
);

CREATE INDEX covid19_table_date_idx ON public.covid19_table USING btree (day_of_data);


--Создание таблицы измерений регионов
create table data_mart.region_dimension(
	region_key serial not null,	
	region_name varchar(100) NULL,
	country_name varchar(100) NULL,
	lat float8 NULL,
	long float8 NULL,
	constraint PK_LOCATION_DIMENSION primary key (region_key)
);


--создание таблицы дат
create table Data_Mart.date_dimension (
	date_key int not null,
	full_date date not null, 
	day_of_week varchar(9),
	day_number_in_week int2,
	last_day_in_week_indicator boolean,
	month_name varchar(9),
	month_number_in_year int2,
	weekday_flag boolean,

constraint PK_DATE_DIMENSION primary key (date_key)
);


--создание таблицы фактов
CREATE TABLE data_mart.fact_covid19 (
	day_of_data date NULL,
	confirmed int8 NULL,
	deaths int4 NULL,
	recovered int4 NULL,
	confrimed_diff int4 NULL,
	deaths_diff int4 NULL,
	recovered_diff int8 NULL,
	active int4 NULL,
	active_diff int4 NULL,
	fatality_rate float8 NULL,
	date_key int4 NULL,
	region_key int2 NULL,
	CONSTRAINT fk_date_dimension FOREIGN KEY (date_key) REFERENCES Data_Mart.date_dimension(date_key),
	CONSTRAINT fk_region_dimension FOREIGN KEY (region_key) REFERENCES Data_Mart.region_dimension(region_key)
);
CREATE INDEX fact_covid19_date_idx ON data_mart.fact_covid19 USING btree (day_of_data);



--Создание типов данных для парсинга json 
create type covid19_region_type as (iso varchar(5), lat float8, long float8, name varchar(50), cities text[], province varchar(100));
create type covid19_type as ( date date, active int4, deaths int4, region covid19_region_type, confirmed int4, recovered int4, active_diff int4,
deaths_diff int4, last_update date, fatality_rate float8, confirmed_diff int4, recovered_diff int4 );




-- ПРОЦЕДУРЫ И ФУНКЦИИ

--ТРАНСФОРМАЦИЯ И ОЧИСТКА ДАННЫХ

--создание функции для выбора значений по дате
create or replace function func_covid19_json(date date) 
returns table (js jsonb) as $$
	select json_data -> 'data' as js  from covid19_json cj 
	where date_of_data = date;
$$ language sql;


-- создание процедуры для вставки в таблицу covid19_table
create or replace procedure insert_in_covid19_table(date date)
as $$

insert into public.covid19_table 
select date,
active,
deaths,
confirmed,
recovered,
active_diff,
deaths_diff,
confirmed_diff,
recovered_diff,
fatality_rate,
(region).province,
(region).name,
(region).lat,
(region).long from func_covid19_json(date),
jsonb_populate_recordset(null::covid19_type, js) as js

$$
language sql;

-- call insert_in_covid19_table('2023-01-14');


--Процедура заполения таблицы region_dimension ВЫПОЛНЯТЬ ОДИН РАЗ!
create or replace procedure insert_region_dimension()
language sql
as $$

insert into data_mart.region_dimension (region_name, country_name, lat, long)
select region_name, country_name, lat, long from public.covid19_table;

$$;


-- Процедура наполения внешним ключом таблицы covid19_table
create or replace procedure insert_fk_in_covid19_table(date date)
as $$

-- Заполнение date_key таблицы covid19_table
update public.covid19_table
set date_key  = (concat(extract(year from date), 
case when extract(month from date) < 10 
	then concat('0', extract(month from date)::text) else extract(month from date)::text end,
case when extract(day from date) < 10 
	then concat('0', extract(day from date)::text) else extract(day from date)::text end))::int
Where day_of_data = date;
	
-- Заполнение region_key таблицы covid19_table
update public.covid19_table ct
set region_key = rd.region_key
from data_mart.region_dimension rd
where rd.region_name = ct.region_name AND ct.day_of_data = date;

$$
language sql;

-- ЗАГРУЗКА ДАННЫХ

-- Процедура заполнения таблицы фактов fact_covid19
create or replace procedure insert_in_fact_covid19(date date)
as $$

insert into data_mart.fact_covid19 
select day_of_data, 
confirmed,
deaths ,
recovered ,
confirmed_diff ,
deaths_diff , 
recovered_diff ,
active ,
active_diff ,
fatality_rate ,
date_key ,
region_key 
from public.covid19_table 
where day_of_data = date;

$$
language sql;


--создание функции для наполнения таблицы датами
create or replace function date_dim() returns void as $$

declare
now_date date := '2000/01/01';
end_date date := '2100/12/31';

begin

while now_date <= end_date loop
	
	insert into Data_Mart.date_dimension
	(
	date_key,
	full_date,
	day_of_week,
	day_number_in_week,
	last_day_in_week_indicator,
	month_name,
	month_number_in_year,
	weekday_flag)
	
	values
	(
	(concat(extract(year from now_date), 
case when extract(month from now_date) < 10 
	then concat('0', extract(month from now_date)::text) else extract(month from now_date)::text end,
case when extract(day from now_date) < 10 
	then concat('0', extract(day from now_date)::text) else extract(day from now_date)::text end))::int, --date_key
	now_date, --full_date
	rtrim(to_char(now_date, 'Day')), --day_of_week,
	extract(isodow from now_date), --day_number_in_week
	(case when extract(isodow from now_date) = 7
    then true else false end)::boolean, -- is_last_day_in_week
    rtrim(to_char(now_date, 'Month')), --month_name
    extract(month from now_date), -- month_number_in_year
    (case when rtrim(to_char(now_date, 'Day'))
    in ('Saturday', 'Sunday')
    then false else true end)::boolean -- is_weekday
	);

now_date := now_date + '1 day'::interval;

end loop;

end;
$$ language plpgsql;


-- ВСТАВКА ДАННЫХ В ТАБЛИЦЫ 

-- Заполнение таблицы dama_mart.date_dimension

select date_dim();

-- Заполнение таблицы data_mart.region_dimension 

INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Anhui','China',31.8257,117.2264),
	 ('Beijing','China',40.1824,116.4142),
	 ('Chongqing','China',30.0572,107.874),
	 ('Fujian','China',26.0789,117.9874),
	 ('Gansu','China',36.0611,103.8343),
	 ('Guangdong','China',23.3417,113.4244),
	 ('Guangxi','China',23.8298,108.7881),
	 ('Guizhou','China',26.8154,106.8748),
	 ('Hainan','China',19.1959,109.7453),
	 ('Hebei','China',38.0428,114.5149);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Heilongjiang','China',47.862,127.7615),
	 ('Henan','China',33.882,113.614),
	 ('Hong Kong','China',22.3,114.2),
	 ('Hubei','China',30.9756,112.2707),
	 ('Hunan','China',27.6104,111.7088),
	 ('Inner Mongolia','China',44.0935,113.9448),
	 ('Jiangsu','China',32.9711,119.455),
	 ('Jiangxi','China',27.614,115.7221),
	 ('Jilin','China',43.6661,126.1923),
	 ('Liaoning','China',41.2956,122.6085);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Macau','China',22.1667,113.55),
	 ('Ningxia','China',37.2692,106.1655),
	 ('Qinghai','China',35.7452,95.9956),
	 ('Shaanxi','China',35.1917,108.8701),
	 ('Shandong','China',36.3427,118.1498),
	 ('Shanghai','China',31.202,121.4491),
	 ('Shanxi','China',37.5777,112.2922),
	 ('Sichuan','China',30.6171,102.7103),
	 ('Tianjin','China',39.3054,117.323),
	 ('Tibet','China',31.6927,88.0924);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Xinjiang','China',41.1129,85.2401),
	 ('Yunnan','China',24.974,101.487),
	 ('Zhejiang','China',29.1832,120.0934),
	 ('Adygea Republic','Russia',44.6939006,40.1520421),
	 ('Altai Krai','Russia',52.6932243,82.69314240000001),
	 ('Altai Republic','Russia',50.7114101,86.8572186),
	 ('Amur Oblast','Russia',52.8032368,128.437295),
	 ('Arkhangelsk Oblast','Russia',63.5589686,43.1221646),
	 ('Astrakhan Oblast','Russia',47.1878186,47.608851),
	 ('Bashkortostan Republic','Russia',54.8573563,57.1439682);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Belgorod Oblast','Russia',50.7080119,37.5837615),
	 ('Bryansk Oblast','Russia',52.8873315,33.41585300000001),
	 ('Buryatia Republic','Russia',52.7182426,109.492143),
	 ('Chechen Republic','Russia',43.39761470000001,45.6985005),
	 ('Chelyabinsk Oblast','Russia',54.4223954,61.1865846),
	 ('Chukotka Autonomous Okrug','Russia',66.0006475,169.4900869),
	 ('Chuvashia Republic','Russia',55.4259922,47.0849429),
	 ('Dagestan Republic','Russia',43.05749160000001,47.1332224),
	 ('Ingushetia Republic','Russia',43.11542075,45.01713552),
	 ('Irkutsk Oblast','Russia',56.6370122,104.719221);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Ivanovo Oblast','Russia',56.9167446,41.43521370000001),
	 ('Jewish Autonomous Okrug','Russia',48.57527615,132.66307460000002),
	 ('Kabardino-Balkarian Republic','Russia',43.4806048,43.5978976),
	 ('Kaliningrad Oblast','Russia',54.7293041,21.1489473),
	 ('Kalmykia Republic','Russia',46.2313018,45.3275745),
	 ('Kaluga Oblast','Russia',54.4382773,35.5272854),
	 ('Kamchatka Krai','Russia',57.1914882,160.03838190000005),
	 ('Karachay-Cherkess Republic','Russia',43.7368326,41.7267991),
	 ('Karelia Republic','Russia',62.61940309999999,33.4920267),
	 ('Kemerovo Oblast','Russia',54.53357809999999,87.342861);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Khabarovsk Krai','Russia',51.6312684,136.121524),
	 ('Khakassia Republic','Russia',53.72258845,91.44293627),
	 ('Khanty-Mansi Autonomous Okrug','Russia',61.0259025,69.0982628),
	 ('Kirov Oblast','Russia',57.9665589,49.4074599),
	 ('Komi Republic','Russia',63.9881421,54.3326073),
	 ('Kostroma Oblast','Russia',58.424756,44.2533273),
	 ('Krasnodar Krai','Russia',45.7684014,39.0261044),
	 ('Krasnoyarsk Krai','Russia',63.3233807,97.0979974),
	 ('Kurgan Oblast','Russia',55.7655302,64.5632681),
	 ('Kursk Oblast','Russia',51.6568453,36.4852695);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Leningrad Oblast','Russia',60.1853296,32.3925325),
	 ('Lipetsk Oblast','Russia',52.6935178,39.1122664),
	 ('Magadan Oblast','Russia',62.48858785,153.9903764),
	 ('Mari El Republic','Russia',56.5767504,47.8817512),
	 ('Mordovia Republic','Russia',54.4419829,44.4661144),
	 ('Moscow','Russia',55.7504461,37.6174943),
	 ('Moscow Oblast','Russia',55.50431579999999,38.0353929),
	 ('Murmansk Oblast','Russia',68.00004179999999,33.9999151),
	 ('Nenets Autonomous Okrug','Russia',68.27557185,57.1686375),
	 ('Nizhny Novgorod Oblast','Russia',55.47180329999999,44.0911594);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('North Ossetia - Alania Republic','Russia',42.79336110000001,44.6324493),
	 ('Novgorod Oblast','Russia',58.2843833,32.5169757),
	 ('Novosibirsk Oblast','Russia',54.9720169,79.48139240000002),
	 ('Omsk Oblast','Russia',56.09352629999999,73.5099936),
	 ('Orel Oblast','Russia',52.96854329999999,36.0692477),
	 ('Orenburg Oblast','Russia',52.02692620000001,54.7276647),
	 ('Penza Oblast','Russia',53.1655415,44.78791810000001),
	 ('Perm Krai','Russia',58.5951603,56.3159546),
	 ('Primorsky Krai','Russia',45.0819456,134.726645),
	 ('Pskov Oblast','Russia',57.5358729,28.8586826);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Rostov Oblast','Russia',47.6222451,40.7957942),
	 ('Ryazan Oblast','Russia',54.42267320000001,40.57052460000001),
	 ('Saint Petersburg','Russia',59.9606739,30.158655100000004),
	 ('Sakha (Yakutiya) Republic','Russia',66.941626,129.642371),
	 ('Sakhalin Oblast','Russia',49.7219665,143.448533),
	 ('Samara Oblast','Russia',53.2128813,50.8914633),
	 ('Saratov Oblast','Russia',51.6520555,46.86319520000001),
	 ('Smolensk Oblast','Russia',55.03434960000001,33.0192065),
	 ('Stavropol Krai','Russia',44.86325770000001,43.4406913),
	 ('Sverdlovsk Oblast','Russia',58.6414755,61.8021546);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Tambov Oblast','Russia',52.9019574,41.3578918),
	 ('Tatarstan Republic','Russia',55.7648572,52.43104273),
	 ('Tomsk Oblast','Russia',58.6124279,82.04753149999998),
	 ('Tula Oblast','Russia',53.9570701,37.3690909),
	 ('Tver Oblast','Russia',57.1134475,35.174442799999994),
	 ('Tyumen Oblast','Russia',58.8206488,70.36588370000001),
	 ('Tyva Republic','Russia',51.4017149,93.8582593),
	 ('Udmurt Republic','Russia',57.1961165,52.69598320000001),
	 ('Ulyanovsk Oblast','Russia',54.1463177,47.2324921),
	 ('Vladimir Oblast','Russia',56.0503336,40.6561633);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Volgograd Oblast','Russia',49.6048339,44.29035820000001),
	 ('Roraima','Brazil',-2.7376,-62.0751),
	 ('Vologda Oblast','Russia',60.0391461,43.1215213),
	 ('Voronezh Oblast','Russia',50.9800393,40.15065070000001),
	 ('Yamalo-Nenets Autonomous Okrug','Russia',67.1471631,74.3415488),
	 ('Yaroslavl Oblast','Russia',57.77819760000001,39.0021095),
	 ('Zabaykalsky Krai','Russia',52.248521,115.956325),
	 ('Washington','US',47.4009,-121.4905),
	 ('Illinois','US',40.3495,-88.9861),
	 ('California','US',36.1162,-119.6816);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Arizona','US',33.7298,-111.4312),
	 ('New York','US',42.1657,-74.9481),
	 ('Massachusetts','US',42.2302,-71.5301),
	 ('Diamond Princess','US',35.4437,139.638),
	 ('Grand Princess','US',37.6489,-122.6655),
	 ('Georgia','US',33.0406,-83.6431),
	 ('Colorado','US',39.0598,-105.3111),
	 ('Florida','US',27.7663,-81.6868),
	 ('New Jersey','US',40.2989,-74.521),
	 ('Oregon','US',44.572,-122.0709);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Texas','US',31.0545,-97.5635),
	 ('Pennsylvania','US',40.5908,-77.2098),
	 ('Iowa','US',42.0115,-93.2105),
	 ('Maryland','US',39.0639,-76.8021),
	 ('North Carolina','US',35.6301,-79.8064),
	 ('South Carolina','US',33.8569,-80.945),
	 ('Tennessee','US',35.7478,-86.6923),
	 ('Virginia','US',37.7693,-78.17),
	 ('Indiana','US',39.8494,-86.2583),
	 ('Kentucky','US',37.6681,-84.6701);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('District of Columbia','US',38.8974,-77.0268),
	 ('Nevada','US',38.3135,-117.0554),
	 ('New Hampshire','US',43.4525,-71.5639),
	 ('Minnesota','US',45.6945,-93.9002),
	 ('Nebraska','US',41.1254,-98.2681),
	 ('Ohio','US',40.3888,-82.7649),
	 ('Rhode Island','US',41.6809,-71.5118),
	 ('Wisconsin','US',44.2685,-89.6165),
	 ('Connecticut','US',41.5978,-72.7554),
	 ('Hawaii','US',21.0943,-157.4983);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Oklahoma','US',35.5653,-96.9289),
	 ('Utah','US',40.15,-111.8624),
	 ('Kansas','US',38.5266,-96.7265),
	 ('Louisiana','US',31.1695,-91.8678),
	 ('Missouri','US',38.4561,-92.2884),
	 ('Vermont','US',44.0459,-72.7107),
	 ('Alaska','US',61.3707,-152.4044),
	 ('Arkansas','US',34.9697,-92.3731),
	 ('Delaware','US',39.3185,-75.5071),
	 ('Idaho','US',44.2405,-114.4788);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Maine','US',44.6939,-69.3819),
	 ('Michigan','US',43.3266,-84.5361),
	 ('Mississippi','US',32.7416,-89.6787),
	 ('Montana','US',46.9219,-110.4544),
	 ('New Mexico','US',34.8405,-106.2485),
	 ('North Dakota','US',47.5289,-99.784),
	 ('South Dakota','US',44.2998,-99.4388),
	 ('West Virginia','US',38.4912,-80.9545),
	 ('Wyoming','US',42.756,-107.3025),
	 ('Alabama','US',32.3182,-86.9023);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Puerto Rico','US',18.2208,-66.5901),
	 ('Guam','US',13.4443,144.7937),
	 ('Virgin Islands','US',18.3358,-64.8963),
	 ('American Samoa','US',-14.270999999999999,-170.132),
	 ('Northern Mariana Islands','US',15.0979,145.6739),
	 ('Andaman and Nicobar Islands','India',11.225999,92.968178),
	 ('Andhra Pradesh','India',15.9129,79.74),
	 ('Arunachal Pradesh','India',27.768456,96.384277),
	 ('Assam','India',26.357149,92.830441),
	 ('Bihar','India',25.679658,85.60484);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Chandigarh','India',30.733839,76.76827800000002),
	 ('Chhattisgarh','India',21.264705,82.035366),
	 ('Delhi','India',28.646519,77.10898),
	 ('Goa','India',15.359682,74.057396),
	 ('Gujarat','India',22.694884,71.590923),
	 ('Haryana','India',29.20004,76.332824),
	 ('Himachal Pradesh','India',31.927213,77.233081),
	 ('Jammu and Kashmir','India',33.75943,76.612638),
	 ('Jharkhand','India',23.654536,85.557631),
	 ('Karnataka','India',14.70518,76.166436);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Kerala','India',10.450898,76.405749),
	 ('Ladakh','India',34.1526,77.5771),
	 ('Madhya Pradesh','India',23.541513,78.289633),
	 ('Maharashtra','India',19.449759,76.108221),
	 ('Manipur','India',24.738975,93.882541),
	 ('Meghalaya','India',25.536934,91.278882),
	 ('Mizoram','India',23.309381,92.83822),
	 ('Nagaland','India',26.06702,94.470302),
	 ('Odisha','India',20.505428,84.418059),
	 ('Puducherry','India',11.882658,78.86498);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Punjab','India',30.841465000000003,75.40879),
	 ('Rajasthan','India',26.583423,73.847973),
	 ('Sikkim','India',27.571671,88.472712),
	 ('Tamil Nadu','India',11.006091,78.400624),
	 ('Telangana','India',18.1124,79.0193),
	 ('Tripura','India',23.746783,91.743565),
	 ('Uttar Pradesh','India',26.925425,80.560982),
	 ('Uttarakhand','India',30.156447,79.197608),
	 ('West Bengal','India',23.814082,87.979803),
	 ('Dadra and Nagar Haveli and Daman and Diu','India',20.194742,73.080901);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Lakshadweep','India',13.6999972,72.18333259999999),
	 ('Acre','Brazil',-9.0238,-70.812),
	 ('Alagoas','Brazil',-9.5713,-36.782),
	 ('Amapa','Brazil',0.902,-52.003),
	 ('Amazonas','Brazil',-3.4168,-65.8561),
	 ('Bahia','Brazil',-12.5797,-41.7007),
	 ('Ceara','Brazil',-5.4984,-39.3206),
	 ('Distrito Federal','Brazil',-15.7998,-47.8645),
	 ('Espirito Santo','Brazil',-19.1834,-40.3089),
	 ('Goias','Brazil',-15.827,-49.8362);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Maranhao','Brazil',-4.9609,-45.2744),
	 ('Mato Grosso','Brazil',-12.6819,-56.9211),
	 ('Mato Grosso do Sul','Brazil',-20.7722,-54.7852),
	 ('Minas Gerais','Brazil',-18.5122,-44.555),
	 ('Para','Brazil',-1.9981,-54.9306),
	 ('Paraiba','Brazil',-7.24,-36.782),
	 ('Parana','Brazil',-25.2521,-52.0215),
	 ('Pernambuco','Brazil',-8.8137,-36.9541),
	 ('Piaui','Brazil',-7.7183,-42.7289),
	 ('Rio Grande do Norte','Brazil',-5.4026,-36.9541);
INSERT INTO data_mart.region_dimension (region_name,country_name,lat,long) VALUES
	 ('Rio Grande do Sul','Brazil',-30.0346,-51.2177),
	 ('Rio de Janeiro','Brazil',-22.9068,-43.1729),
	 ('Rondonia','Brazil',-11.5057,-63.5806),
	 ('Santa Catarina','Brazil',-27.2423,-50.2189),
	 ('Sao Paulo','Brazil',-23.5505,-46.6333),
	 ('Sergipe','Brazil',-10.5741,-37.3857),
	 ('Tocantins','Brazil',-10.1753,-48.2982);

