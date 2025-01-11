// AMR 
// Factset and Compustat

// Install functions
ssc install winsor
help winsor2 
help summarize

//Import Compustat data
	import delimited "Compustat.csv"
	browse

// drop if missing datadate
	browse if datadate == .

	// drop if missing fqtr	
	browse if fqtr == .
	summarize fyearq if fqtr == .
	drop if fqtr == .

// Create a Unique identifier for focalfirm - year (to collapse the data by focalfirm-year)
	egen uniquefocalyearqtr = group (gvkey fyearq fqtr)
	duplicates report uniquefocalyearqtr

// Clean duplicate observations 
	// Check for duplicates
	duplicates report gvkey fyearq fqtr 	// duplicates - yes
	
	// Mark the duplicates	
		// check duplicates by all variables
		unab vlist : _all
		sort `vlist' 
		quietly by `vlist' : gen dup = cond(_N==1,0,_n)
		summarize dup
		tabulate dup 	// no duplicates - there are differences in variables 
		drop dup
		
		// check duplicates by gvkey fyearq fqtr	
		sort gvkey fyearq fqtr
		quietly by gvkey fyearq fqtr: gen dup = cond(_N==1,0,_n)
		summarize dup
		tabulate dup 	// duplicate if dup>0 
		
		// Check individual observations
		browse if dup > 0		
		// Drop the duplicate observation
		drop if dup > 1
		browse if dup > 0		
		drop dup
		
		// check duplicates by gvkey fyearq fqtr 	
		sort gvkey fyearq fqtr
		quietly by gvkey fyearq fqtr: gen dup = cond(_N==1,0,_n)
		summarize dup
		tabulate dup 	// no duplicates 
		drop dup

// Variables (q- quarterly; y - yearly)
// AQCY -- Acquisitions
// CAPXY -- Capital Expenditures
// CHQ -- Cash ; 
// CIQ -- Comprehensive Income - Total ; CIY -- Comprehensive Income - Total ; 
// COGSQ -- Cost of Goods Sold ; COGSY -- Cost of Goods Sold
// CSHIQ -- Common Shares Issued 
// DLTTQ -- Long-Term Debt - Total ; 
// DOY -- Discontinued Operations
// ICAPTQ -- Invested Capital - Total - Quarterly ; 
// INVCHY -- Inventory - Decrease (Increase)
// LCTQ -- Current Liabilities - Total ; LLTQ -- Long-Term Liabilities (Total) ; LTQ -- Liabilities - Total ; 
// MKVALTQ -- Market Value - Total
// NIQ -- Net Income (Loss)
// PPEGTQ -- Property, Plant and Equipment - Total (Gross) - Quarterly ; PPENTQ -- Property Plant and Equipment - Total (Net)
// PRCCQ -- Price Close - Quarter ; PRCHQ -- Price High - Quarter ; PRCLQ -- Price Low - Quarter 
// RDIPQ -- In Process R&D ; RDIPY -- In Process R&D ; XRDQ -- Research and Development Expense ; 	XRDY -- Research and Development Expense
// RECTQ -- Receivables - Total ; 
// REVTQ -- Revenue - Total ; REVTY -- Revenue - Total
// SALEQ -- Sales/Turnover (Net) ; SALEY -- Sales/Turnover (Net)
// SCSTKCY -- Sale of Common Stock (Cash Flow) ; 
// SEQQ -- Stockholders Equity > Parent > Index Fundamental > Quarterly ; TEQQ -- Stockholders Equity - Total
// SETAQ -- Settlement (Litigation/Insurance) After-tax ; SETPQ -- Settlement (Litigation/Insurance) Pretax ; 
// SPPEY -- Sale of Property 
// TXDIY -- Income Taxes - Deferred ; TXPDY -- Income Taxes Paid ; TXTY -- Income Taxes - Total
// TFVAQ -- Total Fair Value Assets ; TFVLQ -- Total Fair Value Liabilities 
// TXTQ -- Income Taxes - Total ; 
// UGIQ -- Gross Income (Income Before Interest Charges)
// UINVQ -- Inventories
// WCAPQ -- Working Capital (Balance Sheet)
// XOPRY -- Operating Expense- Total 
// IBQ -- Income Before Extraordinary Items

// Check basic variables - // normal? ; negative values imply that we cannot take logs 
	
	// Net Income = niq
		hist niq						// normal?
		summarize niq					// negative values so cannot take logs 
	// Stockholders Equity = seqq		
		hist seqq						// normal?
		summarize seqq					// negative values so cannot take logs 
	// Invested Capital - Total - Quarterly = icaptq
		hist icaptq					// normal? 
		summarize icaptq
	// OIBDPQ -- Operating Income Before Depreciation - Quarterly
		hist oibdpq						
		summarize oibdpq					 
	// Income Before Extraordinary Items = ibq
		hist ibq						
		summarize ibq					 
		// Winsorize ibq 
			winsor2 ibq, replace cuts(1 99)
			hist ibq					
			summarize ibq 			
	// Dividends - Preferred/Preference = dvpq
		hist dvpq						
		summarize dvpq					 
		// Winsorize dvpq 
			winsor2 dvpq, replace cuts(1 99)
			hist dvpq					
			summarize dvpq 			
	// Common/Ordinary Equity = ceqq
		hist ceqq						
		summarize ceqq					 
		// Winsorize ceqq 
			winsor2 ceqq, replace cuts(1 99)
			hist ceqq					
			summarize ceqq 			
	// Price close = PRCCQ
		hist prccq 
		summarize prccq 					 
		// Winsorize prccq  
			winsor2 prccq, replace cuts(1 99)
			hist prccq					
			summarize prccq 			
	// Common Shares Outstanding = CSHOQ
		hist cshoq 
		summarize cshoq 					 
		// Winsorize prccq  
			winsor2 cshoq, replace cuts(1 99)
			hist cshoq					
			summarize cshoq 			
			
// Variables for analysis
	sort gvkey fyearq fqtr

	// Assets total = atq
		hist atq						
		summarize atq 					 
	
	// Lagged latq 
		by gvkey: gen latq = atq[_n-1] 
		browse gvkey fyearq fqtr atq latq 
	
	// log atq
		gen lnatq = log(atq)
		hist lnatq				// normal? 
		summarize lnatq
		browse gvkey fyearq fqtr atq latq lnatq
	
	// lagged log atq
		sort gvkey fyearq fqtr
		by gvkey: gen llnatq = lnatq[_n-1] 
		browse gvkey fyearq fqtr atq latq lnatq llnatq	
	
	// ROA = Operating Income Before Depreciation/Total Assets
		gen roa = oibdp/atq 					// Kim et al 2014
		hist roa							
		summarize roa						
	
	// Winsorize oibdp - OIBDPQ -- Operating Income Before Depreciation - Quarterly
			gen oibdpqw = oibdpq
			winsor2 oibdpqw, replace cuts(1 99)
			hist oibdpqw					
			summarize oibdpqw 			 		// negative values so cannot take logs
	
	// Winsorize atq 
			gen atqw = atq
			winsor2 atqw, replace cuts(1 99)
			hist atqw						
			summarize atqw 					
			// Drop if < 20   				// Only considering medium to large firms 
				drop if atqw < 20
				hist atqw						
				summarize atqw 		
		
	// Winsorized roa
			gen roaw = oibdpqw/atqw 				// Kim et al 2014
			hist roaw							
			summarize roaw						
		// Winsorize roa 
		//	winsor2 roa, replace cuts(1 99)
		//	hist roa						
		//	summarize roa 					 
			// Lagged roa 
			//	by gvkey: gen lroa = roa[_n-1] 
			//	browse gvkey fyearq fqtr roa lroa 
	
	// ROE = (Income Before Extraordinary Items - (Dividends - Preferred/Preference))/(Common/Ordinary Equity) 
		// Check Dividends = dvpq
			hist dvpq 
			summarize dvpq 
			summarize dvpq if dvpq == 0
			summarize dvpq if dvpq == .
		// Check Income Before Extraordinary Items = ibq
			hist ibq 
			summarize ibq 
			summarize ibq if ibq == 0
			summarize ibq if ibq == .
		// ROE
		gen roe = (ibq - dvpq) /ceqq  		// Kim et al 2014
		hist roe							 
		summarize roe	
			// Winsorize roe
			gen roew = roe
			winsor2 roew, replace cuts(1 99)
			hist roew						
			summarize roew					 
			// Lagged roe 
				by gvkey: gen lroew = roew[_n-1] 
				browse gvkey fyearq fqtr roew lroew 
	
	// Sales = saleq
		hist saleq							 
		summarize saleq						 
			// Winsorize saleq
			gen saleqw = saleq
			winsor2 saleqw, replace cuts(1 99)
			hist saleqw						
			summarize saleqw 					 
				// Size controlled
				gen saleqwbyatq = saleqw/atq 		
				hist saleqwbyatq 				
				summarize saleqwbyatq 			
					// Lagged saleqbyatq 
					by gvkey: gen lsaleqwbyatq = saleqwbyatq[_n-1] 
					browse gvkey fyearq fqtr saleqwbyatq lsaleqwbyatq 
		
	// Sales growth 	
		quitely by gvkey: gen salesgrowth = (saleq[_n+3] - saleq)/saleq
	
	// Revenue = revtq
		hist revtq							 
		summarize revtq 					 
			// Winsorize revtq
			gen revtqw = revtq
			winsor2 revtqw, replace cuts(1 99)
			hist revtqw						
			summarize revtqw 					 
				// Size controlled
				gen revtqwbyatq = revtqw/atq 		
				hist revtqwbyatq 				
				summarize revtqwbyatq 			
					// Lagged revtqbyatq 
					by gvkey: gen lrevtqwbyatq = revtqwbyatq[_n-1] 
					browse gvkey fyearq fqtr revtqwbyatq lrevtqwbyatq 
	
	// COGS = cogsq
		hist cogsq							 
		summarize cogsq
			// Winsorize cogsq
			gen cogsqw = cogsq
			winsor2 cogsqw, replace cuts(1 99)
			hist cogsqw						
			summarize cogsqw 					 
				// Size controlled by assets
				gen cogsqwbyatq = cogsqw/atqw 		
				hist cogsqwbyatq 				
				summarize cogsqwbyatq 	
					// Lagged cogsqbyatq 
					by gvkey: gen lcogsqwbyatq = cogsqwbyatq[_n-1] 
					browse gvkey fyearq fqtr cogsqwbyatq lcogsqwbyatq 
	
	// Inventory total = invtq
		hist invtq							
		summarize invtq 					
			// Winsorize invtq
			gen invtqw = invtq
			winsor2 invtqw, replace cuts(1 99)
			hist invtqw						
			summarize invtqw 					 
				// Size controlled by sales
				gen invtqwbysales = invtqw/saleqw 	
				hist invtqwbysales 				
				summarize invtqwbysales 	
					// Lagged invbysales 
					by gvkey: gen linvtqwbysales = invtqwbysales[_n-1] 
					browse gvkey fyearq fqtr invtqwbysales linvtqwbysales 
				// Size controlled by assets
				gen invtqwbyatq = invtqw/atqw	
				hist invtqwbyatq 
				summarize invtqwbyatq 
					// Lagged invbyassets 
					by gvkey: gen linvtqwbyatq = invtqwbyatq[_n-1] 
					browse gvkey fyearq fqtr invtqwbyatq linvtqwbyatq 
					
	// Inventory turns = COGS/Inventories total = cogsq/invtq
		gen invturn = cogsq / invtq
		hist invturn							
		summarize invturn 					
			// Winsorize invtq
			gen invturnw = invturn
			winsor2 invturnw, replace cuts(1 99)
			hist invturnw						
			summarize invturnw 					 
	
	// Change in Inventory = Inventories total q - Inventories total q-1 
		gen changeinvtq = invtq - invtq[_n-1]
		hist changeinvtq							
		summarize changeinvtq

**	// Average inventory (annual) = Sum (Inventory)	/ 4

	// R&D expense = xrdq 
		hist xrdq					 
		summarize xrdq
			// Winsorize xrdq
			gen xrdqw = xrdq
			winsor2 xrdqw, replace cuts(1 99)
			hist xrdqw						
			summarize xrdqw 					
				// Size controlled
				gen xrdqwbyatq = xrdqw/atq 		
				hist xrdqwbyatq 				
				summarize xrdqwbyatq 	
					// Lagged xrdqbyatq 
					by gvkey: gen lxrdqwbyatq = xrdqwbyatq[_n-1] 
					browse gvkey fyearq fqtr xrdqwbyatq lxrdqwbyatq 
	
	// B/M ratio 
		// Market value = mv1 = Price close * Common Shares Outstanding
		gen mv1 = prccq * cshoq
		drop if mv1 < 1
		// Book value per share = bv1 = ceqq
		// Calculate B/M
		gen bmr = ceqq/mv1
			// Winsorize xrdq
			gen bmrw = bmr
			winsor2 bmrw, replace cuts(1 99)
			hist bmrw						
			summarize bmrw 	
				// Lagged bmr 
				by gvkey: gen lbmrw = bmrw[_n-1] 
				browse gvkey fyearq fqtr bmrw lbmrw 

			
	** // CAPEX (cumulative) - Arik to do 
		hist capxy 								
		summarize capxy 					
			// Winsorize capxy
			winsor2 capxy, replace cuts(1 99)
			hist capxy						
			summarize capxy 					 
				// Size controlled
				gen capxbyatq = capxy/atq 		
				hist capxbyatq 					
				summarize capxbyatq 
					// Lagged capxbyatq 
					by gvkey: gen lcapxbyatq = capxbyatq[_n-1] 
					browse gvkey fyearq fqtr capxbyatq lcapxbyatq 

					
					
				
// drop variables
	drop indfmt consol popsrc datafmt curcdq
	drop busdesc city spcindcd spcseccd
	
//	export the data
	export delimited using "Compustat Cleam.csv", replace
	
	xtset uniquefocalsupplier year
	xtdescribe	
	

	
	
