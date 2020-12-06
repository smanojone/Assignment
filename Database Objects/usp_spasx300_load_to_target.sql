CREATE OR REPLACE PROCEDURE public.spasx300_load_to_target()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    
BEGIN
    update	spasx300_detail
set		accurate_record_flag  = 'FALSE',
		update_timestamp = current_timestamp 
from	spasx300_detail sd inner join spasx300_staging ss 
on		sd.stock_key = ss."STOCK KEY"  and sd.effective_date  = cast(ss."EFFECTIVE DATE"::text as date) ;


/*From the staging table the data is moved to permanent table. This allows for further transformation, updates. All the loads are captured in this table
 * and can be identified using the accurate_record_flag, insert_timestamp, update_timestamp 
 */

INSERT INTO spasx300_detail
	(
	index_name, 
	index_code, 
	index_key, 
	effective_date, 
	company, 
	ric, 
	bloomberg_ticker, 
	cusip, 
	isin, 
	sedol, 
	ticker, 
	gv_key, 
	stock_key, 
	gics_code, 
	dji_industry_code, 
	alternate_classification_code, 
	mic, 
	country_of_domicile, 
	country_of_listing, 
	region, 
	"size", 
	cap_range, 
	currency_code, 
	local_price, 
	fx_rate, 
	shares_outstanding, 
	market_cap, 
	iwf, 
	awf, 
	growth, 
	value, 
	index_shares, 
	index_market_cap, 
	index_weight,
	daily_price_return, 
	daily_total_return, 
	dividend, 
	net_dividend, 
	accurate_record_flag,
	insert_timestamp,
	update_timestamp)
select
	"INDEX NAME", 
	"INDEX CODE", 
	"INDEX KEY", 
	cast(ss."EFFECTIVE DATE"::text as date), 
	"COMPANY", 
	"RIC", 
	"BLOOMBERG TICKER", 
	"CUSIP" , 
	"ISIN", 
	"SEDOL", 
	"TICKER", 
	"GV KEY", 
	"STOCK KEY", 
	"GICS CODE", 
	"DJI INDUSTRY CODE", 
	"ALTERNATE CLASSIFICATION CODE", 
	"MIC", 
	"COUNTRY OF DOMICILE", 
	"COUNTRY OF LISTING", 
	"REGION", 
	"SIZE", 
	"CAP RANGE", 
	"CURRENCY CODE", 
	"LOCAL PRICE", 
	"FX RATE",
	"SHARES OUTSTANDING", 
	"MARKET CAP", 
	"IWF", 
	"AWF", 
	"GROWTH", 
	"VALUE", 
	"INDEX SHARES",
	"INDEX MARKET CAP",
	"INDEX WEIGHT", 
	"DAILY PRICE RETURN", 
	"DAILY TOTAL RETURN", 
	"DIVIDEND",
	"NET DIVIDEND",
	'TRUE',
	current_timestamp ,
	null
FROM spasx300_staging ss;

/*For the next load, clearing the staging table*/


truncate table spasx300_staging;
	
/*NOT Implemented but deginitely should be - 
 * Delete the target table spasx300_detail with the old data (if required). This number of years of data to be maintained can be managed in a configuration db table
 * */

    
END;
$procedure$
;
