/*View for the users that give them the spasx300 snapshot for every effective day*/

create view spasx300_snapshot_view
as 
SELECT 	index_name, 
		index_code, 
		index_key, 
		effective_date, 
		company, ric, 
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
		insert_timestamp
FROM 	spasx300_detail 
where	accurate_record_flag = 'TRUE';


