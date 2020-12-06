-- public.spasx300_detail definition

-- Drop table

-- DROP TABLE public.spasx300_detail;

CREATE TABLE public.spasx300_detail (
	index_name varchar(15) NOT NULL,
	index_code numeric NOT NULL,
	index_key varchar(30) NOT NULL,
	effective_date date NOT NULL,
	company varchar(50) NOT NULL,
	ric varchar(25) NOT NULL,
	bloomberg_ticker varchar(25) NULL,
	cusip numeric NULL,
	isin varchar(15) NULL,
	sedol numeric NULL,
	ticker varchar(5) NULL,
	gv_key varchar(15) NULL,
	stock_key numeric NULL,
	gics_code numeric NULL,
	dji_industry_code varchar(25) NULL,
	alternate_classification_code numeric NULL,
	mic varchar(5) NULL,
	country_of_domicile varchar(5) NULL,
	country_of_listing varchar(5) NULL,
	region numeric NULL,
	"size" numeric NULL,
	cap_range numeric NULL,
	currency_code varchar(3) NULL,
	local_price numeric(20,3) NULL,
	fx_rate numeric(20,5) NULL,
	shares_outstanding numeric NULL,
	market_cap numeric NULL,
	iwf numeric(10,3) NULL,
	awf numeric NULL,
	growth numeric NULL,
	value numeric NULL,
	index_shares numeric NULL,
	index_market_cap numeric NULL,
	index_weight numeric(30,25) NULL,
	daily_price_return numeric(30,25) NULL,
	daily_total_return numeric(30,25) NULL,
	dividend numeric NULL,
	net_dividend numeric NULL,
	accurate_record_flag varchar(5) NOT NULL,
	insert_timestamp timestamp NULL,
	update_timestamp timestamp NULL
);